import paramiko
import time
import threading
import logging
import re
from proxmox_client import connect_to_proxmox
from disk_service import migration_status, update_migration_status, copy_disk_data, get_migration_status
from utils import get_vm_info, validate_migration_data
from models import Cluster

logger = logging.getLogger(__name__)

def migrate_vm(data):
    """Main VM migration function"""
    
    try:
        # Clear any previous migration details and reset counters
        migration_status['details'] = []
        migration_status['current_disk'] = 0
        migration_status['total_disks'] = 0
        migration_status['needs_confirmation'] = False
        migration_status['stop_confirmed'] = False
        migration_status['current_migration_log'] = []  # Clear log for new migration
        
        # Initialize migration status using update function
        update_migration_status('initializing', message='Starting migration process...', details='Migration initiated')
        
        # Set additional fields
        migration_status['active'] = True
        migration_status['vmid'] = data.get('vmid', 'unknown')
        
        logger.warning("Starting VM migration process")
        logger.warning(f"Received migration data: {data}")
        
        update_migration_status('validation', message='Validating input data...', stage_progress=50)
        
        # Validate input data
        validate_migration_data(data)
        
        update_migration_status('connecting', message='Connecting to source cluster...', stage_progress=30)
        source_cluster = Cluster.query.get_or_404(data['source_cluster_id'])
        dest_cluster = Cluster.query.get_or_404(data['dest_cluster_id'])
        
        logger.warning(f"Connecting to source cluster: {source_cluster.api_host}")
        source_proxmox = connect_to_proxmox(source_cluster)
        
        update_migration_status('vm_info', message=f"Getting VM information for VMID: {data['vmid']}...", 
                              details=f"Retrieving VM data from node {data['source_node']}")
        logger.warning(f"Getting VM info for VMID: {data['vmid']} on node: {data['source_node']}")
        vm_info = get_vm_info(source_proxmox, data['source_node'], data['vmid'])
        logger.warning(f"VM info: {vm_info}")
        
        update_migration_status('ssh_connection', message='Establishing SSH connection...', stage_progress=20)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        try:
            # Check if VM is running and request confirmation to stop it
            if vm_info['status'] == 'running':
                # Set status to waiting for confirmation
                update_migration_status('confirm_vm_stop', message=f"VM {data['vmid']} is running. Waiting for confirmation to stop...", 
                                      details=f"Requesting user confirmation before stopping VM",
                                      needs_confirmation=True)
                logger.warning(f"VM {data['vmid']} is running. Waiting for user confirmation to stop.")
                
                # Wait for confirmation (handled by the frontend via the API)
                timeout = 300  # 5 minute timeout for confirmation
                start_time = time.time()
                while True:
                    # Check if confirmation was received
                    if migration_status.get('stop_confirmed', False):
                        logger.warning(f"Received confirmation to stop VM {data['vmid']}")
                        update_migration_status('stopping_vm', message=f"Stopping VM {data['vmid']}...", 
                                              details=f"User confirmed. Stopping VM...", stage_progress=10)
                        break
                    
                    # Check if migration was cancelled
                    if not migration_status.get('active', False):
                        logger.warning(f"Migration cancelled while waiting for VM stop confirmation")
                        return
                    
                    # Check for timeout
                    if time.time() - start_time > timeout:
                        raise Exception("Timeout waiting for confirmation to stop VM")
                    
                    # Update status message periodically
                    elapsed = time.time() - start_time
                    if int(elapsed) % 10 == 0:  # Update message every 10 seconds
                        update_migration_status('confirm_vm_stop', 
                                              message=f"VM {data['vmid']} is running. Waiting for confirmation to stop... ({int(timeout - elapsed)}s remaining)", 
                                              details=f"Requesting user confirmation before stopping VM",
                                              needs_confirmation=True)
                    
                    time.sleep(1)
                
                # Now stop the VM after confirmation
                logger.warning(f"Stopping VM {data['vmid']}")
                source_proxmox.nodes(data['source_node']).qemu(data['vmid']).status.stop.post()
                
                timeout = 300  # 5 minutes timeout for stopping
                start_time = time.time()
                while True:
                    status = source_proxmox.nodes(data['source_node']).qemu(data['vmid']).status.current.get()
                    if status['status'] == 'stopped':
                        update_migration_status('vm_stopped', message='VM successfully stopped', 
                                              details='VM is now stopped and ready for migration', stage_progress=100)
                        logger.warning("VM successfully stopped")
                        break
                    if time.time() - start_time > timeout:
                        raise Exception("Timeout waiting for VM to stop")
                    
                    elapsed = time.time() - start_time
                    stop_progress = min(90, (elapsed / timeout) * 100)
                    update_migration_status('stopping_vm', message=f"Waiting for VM to stop... ({int(elapsed)}s)", 
                                          stage_progress=stop_progress)
                    time.sleep(2)
            else:
                update_migration_status('vm_ready', message='VM is already stopped and ready for migration', stage_progress=100)
            
            # Extract hostname from api_host (remove port if present)
            ssh_host = source_cluster.api_host.split(':')[0]
            ssh_port = getattr(source_cluster, 'ssh_port', 22)  # Default to 22 if not set
            update_migration_status('ssh_connecting', message=f"Connecting via SSH to {ssh_host}:{ssh_port}...", stage_progress=30)
            logger.warning(f"Connecting to source cluster via SSH: {ssh_host}:{ssh_port}")
            try:
                ssh.connect(ssh_host, port=ssh_port, username='root', password=source_cluster.ssh_password)
                update_migration_status('ssh_connected', message='SSH connection established successfully', 
                                      details='Ready for data transfer', stage_progress=100)
                logger.info(f"Successfully connected to source cluster via SSH")
            except Exception as ssh_error:
                logger.error(f"SSH connection error: {str(ssh_error)}")
                raise
            sftp = ssh.open_sftp()
            
            update_migration_status('dest_connecting', message='Connecting to destination cluster...', 
                                  details=f'Connecting to {dest_cluster.api_host}', stage_progress=50)
            logger.warning(f"Connecting to destination cluster: {dest_cluster.api_host}")
            dest_proxmox = connect_to_proxmox(dest_cluster)
            
            update_migration_status('config_reading', message='Reading VM configuration...', 
                                  details='Retrieving VM settings and disk information', stage_progress=30)
            logger.warning("Getting source VM configuration")
            vm_config = source_proxmox.nodes(data['source_node']).qemu(data['vmid']).config.get()
            logger.warning(f"Original VM config: {vm_config}")
            
            # Separate disk configurations from VM config
            disk_configs = {}
            vm_config_without_disks = vm_config.copy()
            
            for key, value in vm_config.items():
                # Include standard data disks plus EFI / TPM state disks
                if any(key.startswith(prefix) for prefix in ('scsi', 'virtio', 'ide', 'sata', 'efidisk', 'tpmstate')) and ':' in str(value):
                    disk_configs[key] = value
                    # Remove disk from VM config for initial creation
                    del vm_config_without_disks[key]
                    logger.warning(f"Separated disk config: {key} = {value}")
            
            # Set total disks for progress calculation
            migration_status['total_disks'] = len(disk_configs)
            
            update_migration_status('vm_id_check', message='Checking available VM ID on destination...', 
                                  details=f"Found {len(disk_configs)} disks to migrate", stage_progress=20)
            logger.warning(f"Creating VM on destination node without disks: {data['dest_node']}")
            
            # Find available VM ID if the requested one is taken
            target_vmid = data['vmid']
            max_attempts = 100  # Increased from 10 to 100 attempts
            
            for attempt in range(max_attempts):
                vm_exists = False
                
                # Check if VM with this ID exists on ANY node in the cluster
                try:
                    cluster_resources = dest_proxmox.cluster.resources.get()
                    for resource in cluster_resources:
                        if resource.get('type') == 'qemu' and str(resource.get('vmid')) == str(target_vmid):
                            vm_exists = True
                            logger.warning(f"VM {target_vmid} already exists on node {resource.get('node')}, trying next ID (attempt {attempt + 1}/{max_attempts})")
                            break
                except Exception as e:
                    logger.warning(f"Could not check cluster resources, falling back to node-specific check: {e}")
                    # Fallback to checking specific node if cluster resource check fails
                    try:
                        existing_vm = dest_proxmox.nodes(data['dest_node']).qemu(target_vmid).config.get()
                        vm_exists = True
                        logger.warning(f"VM {target_vmid} already exists on target node, trying next ID (attempt {attempt + 1}/{max_attempts})")
                    except:
                        vm_exists = False
                
                if vm_exists:
                    target_vmid = str(int(target_vmid) + 1)
                    progress = 20 + (attempt / max_attempts) * 30  # 20-50% for ID search
                    update_migration_status('vm_id_check', message=f'VM ID {target_vmid} is taken, checking next...', 
                                          stage_progress=progress)
                else:
                    # VM doesn't exist anywhere in cluster, we can use this ID
                    update_migration_status('vm_id_available', message=f"VM ID {target_vmid} is available", 
                                          details=f"Will use VM ID {target_vmid} for migration", stage_progress=60)
                    logger.warning(f"VM {target_vmid} is available, using it for migration")
                    break
            else:
                raise Exception(f"Could not find available VM ID after {max_attempts} attempts")
            
            # Update data with the new VMID if it changed
            original_vmid = data['vmid']
            if target_vmid != data['vmid']:
                update_migration_status('vm_id_changed', message=f"Changed VM ID from {data['vmid']} to {target_vmid}", 
                                      details=f"Original VM ID was taken", stage_progress=70)
                logger.warning(f"Changed VM ID from {data['vmid']} to {target_vmid}")
                data['vmid'] = target_vmid
            
            # Remove parameters that shouldn't be passed during VM creation
            create_config = vm_config_without_disks.copy()
            for param in ['meta', 'digest', 'vmgenid']:
                if param in create_config:
                    del create_config[param]
                    logger.warning(f"Removed {param} from VM config for creation")
            
            # Add "_migrated" suffix to VM name
            if 'name' in create_config:
                original_name = create_config['name']
                create_config['name'] = f"{original_name}-migrated"
                logger.warning(f"Changed VM name from '{original_name}' to '{create_config['name']}'")
            elif 'hostname' in create_config:
                original_hostname = create_config['hostname']
                create_config['hostname'] = f"{original_hostname}-migrated"
                logger.warning(f"Changed hostname from '{original_hostname}' to '{create_config['hostname']}'")
            
            update_migration_status('vm_creating', message=f"Creating VM {target_vmid} on destination...", 
                                  details=f"Creating VM without disks first", stage_progress=80)
            logger.warning(f"Modified VM config for creation (without disks): {create_config}")
            
            # Create new VM on destination WITHOUT disks
            dest_proxmox.nodes(data['dest_node']).qemu.create(
                vmid=target_vmid,
                **create_config
            )
            update_migration_status('vm_created', message=f"VM {target_vmid} created successfully", 
                                  details=f"VM created without disks", stage_progress=100)
            logger.warning(f"Successfully created VM {target_vmid} without disks")
            
            # Now process each disk separately
            # Filter to only process disks that exist in VM config AND are mapped by user
            storage_mappings = data.get('storage_mappings', {})
            disks_to_migrate = {}
            
            # Only include disks that exist in VM config AND have storage mapping
            for disk_key, disk_config in disk_configs.items():
                if disk_key in storage_mappings:
                    disks_to_migrate[disk_key] = disk_config
                    logger.warning(f"Will migrate disk {disk_key}: {disk_config}")
                else:
                    logger.warning(f"Skipping disk {disk_key} - not in storage mappings")
            
            # Also check if user selected disks that don't exist in VM
            for disk_key in storage_mappings:
                if disk_key not in disk_configs:
                    logger.warning(f"Warning: User selected disk {disk_key} for migration, but it doesn't exist in VM config")
            
            total_disks = len(disks_to_migrate)
            current_disk = 0
            
            # Set total disks for progress calculation
            migration_status['total_disks'] = total_disks
            
            if total_disks == 0:
                logger.warning("No disks to migrate - VM created without disk migration")
                update_migration_status('migration_complete', message='VM migration completed', 
                                      details='VM created successfully (no disks to migrate)', stage_progress=100)
                return {"success": True, "message": f"VM {target_vmid} created successfully without disk migration"}
            
            for disk_key, disk_config in disks_to_migrate.items():
                current_disk += 1
                migration_status['current_disk'] = current_disk
                
                update_migration_status('disk_processing', message=f"Processing disk {current_disk}/{total_disks}: {disk_key}...", 
                                      details=f"Disk config: {disk_config}", stage_progress=5)
                logger.warning(f"Processing disk {disk_key}: {disk_config}")
                
                # Parse disk configuration
                if ':' in disk_config:
                    old_storage = disk_config.split(':')[0]
                    disk_part = disk_config.split(':')[1]
                    disk_file = disk_part.split(',')[0]
                    options = ','.join(disk_part.split(',')[1:]) if ',' in disk_part else ''
                    
                    # Skip CD-ROM and other media types
                    if 'media=cdrom' in options or disk_file.endswith('.iso'):
                        update_migration_status('disk_skipped', message=f"Skipping CD-ROM disk {disk_key}", 
                                              details=f"CD-ROM drives are not migrated", stage_progress=10)
                        logger.warning(f"Skipping CD-ROM disk {disk_key}")
                        continue
                    
                    # Extract size from options (convert to Proxmox API compatible format)
                    size = "20G"  # default size only for regular disks without size info
                    
                    # First priority: get size from disk config options
                    if 'size=' in options:
                        for opt in options.split(','):
                            if opt.startswith('size='):
                                raw_size = opt.split('=')[1]
                                # Convert size to Proxmox API compatible format (whole megabytes)
                                if raw_size.endswith('K'):
                                    # Convert kilobytes to megabytes (minimum 1M for Proxmox API)
                                    kb_value = int(raw_size[:-1])
                                    mb_value = max(1, (kb_value + 1023) // 1024)  # Round up to nearest MB
                                    size = f"{mb_value}M"
                                    logger.warning(f"Converted size {raw_size} to {size} for Proxmox API")
                                elif raw_size.endswith(('M', 'G', 'T')):
                                    size = raw_size
                                    logger.warning(f"Using size from config: {size}")
                                elif raw_size.isdigit():
                                    # Convert bytes to appropriate unit
                                    size_bytes = int(raw_size)
                                    if size_bytes < 1024**2:  # Less than 1MB
                                        mb_value = max(1, (size_bytes + 1024**2 - 1) // (1024**2))  # Round up
                                        size = f"{mb_value}M"
                                    elif size_bytes < 1024**3:  # Less than 1GB  
                                        size = f"{size_bytes // (1024**2)}M"
                                    else:
                                        size_gb = max(1, size_bytes // (1024**3))
                                        size = f"{size_gb}G"
                                    logger.warning(f"Converted byte size {raw_size} to {size}")
                                break
                    
                    # Special handling for EFI disks with efitype parameter (only if no size found)
                    if disk_key.startswith('efidisk') and 'efitype=' in options and size == "20G":
                        for opt in options.split(','):
                            if opt.startswith('efitype='):
                                efi_type = opt.split('=')[1].upper()
                                if efi_type.endswith('M'):
                                    size = efi_type
                                    logger.warning(f"Using efitype size: {size}")
                                break
                    
                    # Determine destination storage for this disk
                    dest_storage = data.get('storage_mappings', {}).get(disk_key)
                    if not dest_storage:
                        # Improved fallback: if user provided at least one mapping, reuse its first storage for unmapped disks
                        if data.get('storage_mappings'):
                            first_mapped_storage = next(iter(data['storage_mappings'].values()))
                            dest_storage = first_mapped_storage
                            logger.warning(f"No storage mapping for {disk_key}, reusing first mapped storage: {dest_storage}")
                        else:
                            # Fallback to old behavior if no mapping provided
                            dest_storage = data.get('dest_storage', 'local')
                            logger.warning(f"No storage mapping for {disk_key}, using fallback dest_storage/local: {dest_storage}")
                    
                    update_migration_status('disk_creating', message=f"Creating disk {disk_key} ({size}) on storage {dest_storage}...", 
                                          details=f"Allocating disk space...", stage_progress=20)
                    logger.warning(f"Creating disk {disk_key} with size {size} on storage {dest_storage}")
                    logger.warning(f"Disk config details: old_storage={old_storage}, disk_file={disk_file}, options={options}")
                    
                    # Create disk on destination storage
                    try:
                        # Extract original disk identifier from source filename to preserve naming
                        original_disk_identifier = None
                        if '/' in disk_file:
                            # For paths like "102/vm-102-disk-1.qcow2" or "vm-102-disk-0"
                            filename_part = disk_file.split('/')[-1]  # Get last part after /
                            if filename_part.startswith('vm-') and '-disk-' in filename_part:
                                # Extract the disk number from original filename
                                parts = filename_part.split('-disk-')
                                if len(parts) > 1:
                                    # Get number and extension if present (e.g., "1.qcow2" or "0")
                                    disk_part = parts[1].split('.')[0]  # Remove extension
                                    if disk_part.isdigit():
                                        original_disk_identifier = disk_part
                        
                        # Fallback: extract disk number from disk key (scsi0 -> 0, efidisk0 -> 0, etc.)
                        if not original_disk_identifier:
                            disk_num = ''.join(filter(str.isdigit, disk_key))
                            original_disk_identifier = disk_num if disk_num else '0'
                        
                        logger.warning(f"Using disk identifier: {original_disk_identifier} (from source: {disk_file})")
                        
                        # Get storage type to determine the correct disk creation method
                        try:
                            # Get storage configuration from cluster storage list
                            storages = dest_proxmox.storage.get()
                            storage_type = 'dir'  # default
                            for storage in storages:
                                if storage.get('storage') == dest_storage:
                                    storage_type = storage.get('type', 'dir')
                                    break
                            logger.warning(f"Destination storage '{dest_storage}' type: {storage_type}")
                        except Exception as e:
                            logger.warning(f"Could not get storage info, assuming dir: {e}")
                            storage_type = 'dir'
                        
                        # Different disk creation approaches based on storage type
                        logger.warning(f"Creating disk for storage type: {storage_type}")
                        
                        if storage_type in ['lvmthin', 'lvm', 'zfspool']:
                            # For LVM/ZFS storage, create with generated filename (no extension)
                            disk_filename = f"vm-{target_vmid}-disk-{original_disk_identifier}"
                            logger.warning(f"Creating LVM/ZFS disk with filename: {disk_filename}")
                            
                            # Check if disk already exists and delete it
                            try:
                                existing_content = dest_proxmox.nodes(data['dest_node']).storage(dest_storage).content.get()
                                for content in existing_content:
                                    volid = content.get('volid', '')
                                    # Check for exact match or conflicting disk with same number but different format
                                    if volid.endswith(disk_filename) or f"vm-{target_vmid}-disk-{original_disk_identifier}." in volid:
                                        logger.warning(f"Found existing/conflicting disk {volid}, deleting it")
                                        dest_proxmox.nodes(data['dest_node']).storage(dest_storage).content(volid).delete()
                            except Exception as e:
                                logger.warning(f"Could not check/delete existing disk: {e}")
                            
                            dest_proxmox.nodes(data['dest_node']).storage(dest_storage).content.create(
                                vmid=target_vmid,
                                filename=disk_filename,
                                size=size,
                                format='raw'
                            )
                            disk_name = disk_filename
                            
                        else:
                            # For file-based storage (dir, nfs, etc.), detect format from source disk
                            original_format = 'qcow2'  # default format for file-based storage
                            
                            # First try to get format from source disk filename
                            if '/' in disk_file and '.' in disk_file:
                                # Extract format from file extension (e.g., vm-101-disk-1.qcow2 -> qcow2)
                                file_parts = disk_file.split('.')
                                if len(file_parts) > 1:
                                    potential_format = file_parts[-1]
                                    if potential_format in ['qcow2', 'raw', 'vmdk', 'vdi']:
                                        original_format = potential_format
                                        logger.warning(f"Detected format from source filename: {original_format}")
                            
                            # Check format option in disk config (overrides filename detection)
                            if ',' in disk_config:
                                for option in disk_config.split(',')[1:]:
                                    if option.startswith('format='):
                                        original_format = option.split('=')[1]
                                        logger.warning(f"Found explicit format option: {original_format}")
                                        break
                            
                            # Use original format and preserve disk numbering from source
                            disk_filename = f"vm-{target_vmid}-disk-{original_disk_identifier}.{original_format}"
                            logger.warning(f"Creating file-based disk with filename: {disk_filename}, format: {original_format}")
                            
                            # Check if disk already exists and delete it
                            try:
                                existing_content = dest_proxmox.nodes(data['dest_node']).storage(dest_storage).content.get()
                                for content in existing_content:
                                    volid = content.get('volid', '')
                                    # Check for exact match or conflicting disk with same number but different format
                                    if volid.endswith(disk_filename) or f"vm-{target_vmid}-disk-{original_disk_identifier}." in volid:
                                        logger.warning(f"Found existing/conflicting disk {volid}, deleting it")
                                        dest_proxmox.nodes(data['dest_node']).storage(dest_storage).content(volid).delete()
                            except Exception as e:
                                logger.warning(f"Could not check/delete existing disk: {e}")
                            
                            dest_proxmox.nodes(data['dest_node']).storage(dest_storage).content.create(
                                vmid=target_vmid,
                                filename=disk_filename,
                                size=size,
                                format=original_format
                            )
                            disk_name = disk_filename
                        
                        update_migration_status('disk_created', message=f"Disk {disk_key} created successfully", 
                                              details=f"Disk storage allocated: {disk_name}", stage_progress=40)
                        logger.warning(f"Successfully created disk storage for {disk_key} with name {disk_name}")
                        
                        # Verify disk was actually created by listing storage content
                        try:
                            storage_content = dest_proxmox.nodes(data['dest_node']).storage(dest_storage).content.get()
                            found_disk = False
                            for content in storage_content:
                                if content.get('volid', '').endswith(disk_name):
                                    found_disk = True
                                    actual_volid = content.get('volid')
                                    logger.warning(f"Found created disk with volid: {actual_volid}")
                                    # Use the actual volid for disk configuration
                                    if ':' in actual_volid:
                                        disk_name = actual_volid.split(':')[1]
                                    break
                            
                            if not found_disk:
                                logger.warning(f"Warning: Could not find created disk {disk_name} in storage content")
                                logger.warning(f"Available storage content: {[c.get('volid', '') for c in storage_content]}")
                        except Exception as e:
                            logger.warning(f"Could not verify disk creation: {e}")
                        
                        # Attach disk to VM - use the exact disk name created
                        new_disk_config = f"{dest_storage}:{disk_name}"
                        if options:
                            # Filter out size option since disk is already created
                            # For EFI/TPM disks, preserve all important options except size
                            filtered_options = []
                            for opt in options.split(','):
                                if not opt.startswith('size='):
                                    # For special disks (EFI/TPM), keep all options except size
                                    if disk_key.startswith(('efidisk', 'tpmstate')):
                                        filtered_options.append(opt)
                                    # For regular disks, filter out some unnecessary options
                                    elif not opt.startswith(('path=', 'file=')):
                                        filtered_options.append(opt)
                            
                            if filtered_options:
                                new_disk_config += ',' + ','.join(filtered_options)
                        
                        update_migration_status('disk_attaching', message=f"Attaching disk {disk_key} to VM...", 
                                              details=f"Disk config: {new_disk_config}", stage_progress=50)
                        logger.warning(f"Attaching disk with config: {new_disk_config}")
                        
                        # Attach the disk to VM
                        update_config = {disk_key: new_disk_config}
                        dest_proxmox.nodes(data['dest_node']).qemu(target_vmid).config.put(**update_config)
                        update_migration_status('disk_attached', message=f"Disk {disk_key} attached to VM", stage_progress=60)
                        logger.warning(f"Successfully attached disk {disk_key} to VM")
                        
                        # Now copy the disk data
                        import uuid
                        copy_id = str(uuid.uuid4())[:8]
                        update_migration_status('disk_copying', message=f"Copying data for disk {disk_key}...", 
                                              details=f"Transferring data to storage", stage_progress=70)
                        logger.warning(f"[COPY-{copy_id}] Starting to copy disk data for {disk_key} to storage {dest_storage}")
                        logger.warning(f"[COPY-{copy_id}] Disk config: {disk_config}")
                        dest_path = copy_disk_data(source_cluster, dest_cluster, data, disk_config, ssh, sftp, 70, dest_storage)
                        logger.warning(f"[COPY-{copy_id}] Completed copying disk data to {dest_path}")
                        update_migration_status('disk_copied', message=f"Disk {disk_key} data copied successfully", 
                                              details=f"Data copied to: {dest_path}", stage_progress=95)
                        logger.warning(f"Successfully copied disk data to {dest_path}")
                        
                    except Exception as disk_error:
                        logger.error(f"Error creating/attaching disk {disk_key}: {str(disk_error)}")
                        migration_status['active'] = False
                        raise disk_error
            
            # Apply network interface mappings if provided
            if data.get('network_mappings'):
                update_migration_status('network_mapping', message='Applying network interface mappings...', 
                                      details='Updating network configuration', stage_progress=30)
                logger.warning(f"Applying network mappings: {data['network_mappings']}")
                
                # Get current VM config to update network interfaces
                current_config = dest_proxmox.nodes(data['dest_node']).qemu(target_vmid).config.get()
                network_updates = {}
                
                for interface, dest_bridge in data['network_mappings'].items():
                    if interface in current_config:
                        # Parse current network config
                        net_config = current_config[interface]
                        # Replace bridge in the configuration
                        import re
                        updated_config = re.sub(r'bridge=[^,]+', f'bridge={dest_bridge}', net_config)
                        network_updates[interface] = updated_config
                        logger.warning(f"Updated {interface}: {net_config} -> {updated_config}")
                
                if network_updates:
                    # Apply network updates
                    dest_proxmox.nodes(data['dest_node']).qemu(target_vmid).config.put(**network_updates)
                    update_migration_status('network_applied', message='Network mappings applied successfully', 
                                          details=f"Updated {len(network_updates)} interfaces", stage_progress=80)
                    logger.warning(f"Applied network updates: {network_updates}")
            
            # Delete old VM if requested
            if data.get('delete_source', False):
                update_migration_status('cleanup', message=f"Deleting source VM {original_vmid}...", 
                                      details=f"Cleaning up source VM...", stage_progress=50)
                logger.warning(f"Deleting source VM {original_vmid}")
                source_proxmox.nodes(data['source_node']).qemu(original_vmid).delete()
                update_migration_status('cleanup_done', message='Source VM deleted successfully', stage_progress=100)
            
            update_migration_status('completed', progress_override=100, message='Migration completed successfully!', 
                                  details=f"VM migrated as ID {target_vmid}")
            logger.warning(f"Migration completed successfully - VM migrated as ID {target_vmid}")
            success_message = f'VM migration completed successfully. VM created with ID {target_vmid}'
            if target_vmid != original_vmid:
                success_message += f' (original ID {original_vmid} was already taken)'
            
            # Mark migration as inactive
            migration_status['active'] = False
            return {'status': 'success', 'message': success_message}
            
        except Exception as e:
            error_msg = f"Error during migration: {str(e)}"
            
            # Cleanup: Delete partially created VM and disks on destination if they exist
            try:
                if 'target_vmid' in locals() and 'dest_proxmox' in locals() and 'data' in locals():
                    update_migration_status('cleanup_error', message='Cleaning up partially created VM and disks...', 
                                          details=f'Deleting VM {target_vmid} and associated disks from destination cluster', stage_progress=25)
                    logger.warning(f"Error occurred, attempting to cleanup VM {target_vmid} on destination")
                    
                    # Try to delete the partially created VM (this also removes attached disks)
                    try:
                        dest_proxmox.nodes(data['dest_node']).qemu(target_vmid).delete()
                        logger.warning(f"Successfully deleted partially created VM {target_vmid} from destination")
                        update_migration_status('cleanup_vm_done', message='VM cleanup completed', 
                                              details=f'Removed incomplete VM {target_vmid}', stage_progress=75)
                    except Exception as cleanup_error:
                        logger.warning(f"Could not delete VM {target_vmid} during cleanup: {cleanup_error}")
                        # Continue with manual disk cleanup if VM deletion failed
                    
                    # Additional cleanup: try to remove any orphaned disks for this VMID
                    try:
                        if 'disk_configs' in locals():
                            # Check all possible storage locations
                            storage_list = data.get('storage_mappings', {})
                            storages_to_check = set(storage_list.values()) if storage_list else {data.get('dest_storage', 'local')}
                            
                            for storage_name in storages_to_check:
                                try:
                                    storage_content = dest_proxmox.nodes(data['dest_node']).storage(storage_name).content.get()
                                    for content in storage_content:
                                        volid = content.get('volid', '')
                                        # Look for disks that belong to our target VMID
                                        if f"vm-{target_vmid}-disk-" in volid:
                                            try:
                                                dest_proxmox.nodes(data['dest_node']).storage(storage_name).content(volid).delete()
                                                logger.warning(f"Deleted orphaned disk: {volid}")
                                            except Exception as disk_del_error:
                                                logger.warning(f"Could not delete orphaned disk {volid}: {disk_del_error}")
                                except Exception as storage_check_error:
                                    logger.warning(f"Could not check storage {storage_name} for orphaned disks: {storage_check_error}")
                                        
                        update_migration_status('cleanup_complete', message='Cleanup completed', 
                                              details=f'Removed incomplete VM {target_vmid} and orphaned disks', stage_progress=100)
                    except Exception as disk_cleanup_error:
                        logger.warning(f"Exception during disk cleanup: {disk_cleanup_error}")
                        pass
                        
            except Exception as cleanup_ex:
                logger.warning(f"Exception during cleanup: {cleanup_ex}")
                pass
            
            migration_status['active'] = False
            migration_status['step'] = 'error'
            migration_status['message'] = error_msg
            logger.error(error_msg)
            logger.exception("Full error traceback:")
            return {'status': 'error', 'message': error_msg}
        
        finally:
            ssh.close()
            
    except Exception as e:
        error_msg = f"Error in migration process: {str(e)}"
        migration_status['active'] = False
        migration_status['step'] = 'error'
        migration_status['message'] = error_msg
        logger.error(error_msg)
        logger.exception("Full error traceback:")
        return {'status': 'error', 'message': error_msg}

