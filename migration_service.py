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
            # Stop VM if running
            if vm_info['status'] == 'running':
                update_migration_status('stopping_vm', message=f"Stopping VM {data['vmid']}...", 
                                      details=f"VM is currently running, stopping...")
                logger.warning(f"Stopping VM {data['vmid']}")
                source_proxmox.nodes(data['source_node']).qemu(data['vmid']).status.stop.post()
                
                timeout = 300  # 5 minutes timeout
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
            update_migration_status('ssh_connecting', message=f"Connecting via SSH to {ssh_host}...", stage_progress=30)
            logger.warning(f"Connecting to source cluster via SSH: {ssh_host}")
            try:
                ssh.connect(ssh_host, username='root', password=source_cluster.ssh_password)
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
                if key.startswith(('scsi', 'virtio', 'ide', 'sata')) and ':' in str(value):
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
                try:
                    # Check if VM with this ID exists
                    existing_vm = dest_proxmox.nodes(data['dest_node']).qemu(target_vmid).config.get()
                    logger.warning(f"VM {target_vmid} already exists, trying next ID (attempt {attempt + 1}/{max_attempts})")
                    target_vmid = str(int(target_vmid) + 1)
                    
                    progress = 20 + (attempt / max_attempts) * 30  # 20-50% for ID search
                    update_migration_status('vm_id_check', message=f'VM ID {target_vmid} is taken, checking next...', 
                                          stage_progress=progress)
                except Exception as e:
                    # VM doesn't exist, we can use this ID
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
            total_disks = len(disk_configs)
            current_disk = 0
            
            for disk_key, disk_config in disk_configs.items():
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
                    
                    # Extract size from options or estimate from disk file
                    size = "20G"  # default size
                    if 'size=' in options:
                        for opt in options.split(','):
                            if opt.startswith('size='):
                                raw_size = opt.split('=')[1]
                                # Ensure size has proper unit
                                if raw_size.isdigit():
                                    # If it's just a number, assume bytes and convert to GB
                                    size_gb = int(raw_size) // (1024**3)
                                    if size_gb < 1:
                                        size_gb = 1
                                    size = f"{size_gb}G"
                                elif raw_size.endswith(('K', 'M', 'G', 'T')):
                                    size = raw_size
                                else:
                                    # Try to parse as bytes
                                    try:
                                        size_bytes = int(raw_size)
                                        size_gb = max(1, size_bytes // (1024**3))
                                        size = f"{size_gb}G"
                                    except ValueError:
                                        size = "20G"  # fallback
                                break
                    
                    # Determine destination storage for this disk
                    dest_storage = data.get('storage_mappings', {}).get(disk_key)
                    if not dest_storage:
                        # Fallback to old behavior if no mapping provided
                        dest_storage = data.get('dest_storage', 'local')
                        logger.warning(f"No storage mapping for {disk_key}, using fallback: {dest_storage}")
                    
                    update_migration_status('disk_creating', message=f"Creating disk {disk_key} ({size}) on storage {dest_storage}...", 
                                          details=f"Allocating disk space...", stage_progress=20)
                    logger.warning(f"Creating disk {disk_key} with size {size} on storage {dest_storage}")
                    logger.warning(f"Disk config details: old_storage={old_storage}, disk_file={disk_file}, options={options}")
                    
                    # Create disk on destination storage
                    try:
                        # Extract disk number from key (scsi0 -> 0, virtio1 -> 1, etc.)
                        disk_num = ''.join(filter(str.isdigit, disk_key))
                        if not disk_num:
                            disk_num = '0'
                        
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
                            disk_filename = f"vm-{target_vmid}-disk-{disk_num}"
                            logger.warning(f"Creating LVM/ZFS disk with filename: {disk_filename}")
                            dest_proxmox.nodes(data['dest_node']).storage(dest_storage).content.create(
                                vmid=target_vmid,
                                filename=disk_filename,
                                size=size,
                                format='raw'
                            )
                            disk_name = disk_filename
                            
                        else:
                            # For file-based storage (dir, nfs, etc.), detect appropriate format
                            # Check what format the original disk uses
                            original_format = 'qcow2'  # default format for file-based storage
                            if ',' in disk_config:
                                for option in disk_config.split(',')[1:]:
                                    if option.startswith('format='):
                                        original_format = option.split('=')[1]
                                        break
                            
                            # Use original format or qcow2 for file-based storage
                            disk_filename = f"vm-{target_vmid}-disk-{disk_num}.{original_format}"
                            logger.warning(f"Creating file-based disk with filename: {disk_filename}, format: {original_format}")
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
                            filtered_options = [opt for opt in options.split(',') if not opt.startswith('size=')]
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

def start_test_migration():
    """Test function to simulate migration for demo purposes"""
    try:
        # Clear any previous migration details and reset counters
        migration_status['details'] = []
        migration_status['current_disk'] = 0
        migration_status['total_disks'] = 2  # Simulate 2 disks
        
        # Initialize using the update function
        update_migration_status('initializing', message='Starting test migration...', details='Initializing test migration')
        
        # Get status and set additional fields
        migration_status['active'] = True
        migration_status['vmid'] = '999'
        
        test_steps = [
            ('validation', 'Validating input data...', 'Input validation complete', 50),
            ('connecting', 'Connecting to source cluster...', 'Connected to source', 80),
            ('vm_info', 'Getting VM information...', 'VM info retrieved', 100),
            ('ssh_connection', 'Establishing SSH connection...', 'SSH connected', 70),
            ('vm_stopped', 'VM stopped successfully', 'VM ready for migration', 100),
            ('dest_connecting', 'Connecting to destination cluster...', 'Connected to destination', 90),
            ('config_reading', 'Reading VM configuration...', 'Config read successfully', 80),
            ('vm_id_available', 'VM ID 999 is available', 'Using VM ID 999', 100),
            ('vm_creating', 'Creating VM on destination...', 'VM created without disks', 50),
            ('vm_created', 'VM created successfully', 'VM ready for disks', 100),
        ]
        
        # Execute basic steps
        for step, message, detail, stage_progress in test_steps:
            status = get_migration_status()
            if not status.get('active'):
                break
                
            update_migration_status(step, message=message, details=detail, stage_progress=stage_progress)
            migration_status['active'] = True  # Keep active flag
            time.sleep(0.8)  # Simulate work
        
        # Simulate disk migration
        for disk_num in range(1, 3):  # 2 disks
            migration_status['current_disk'] = disk_num
            disk_name = f"scsi{disk_num-1}"
            
            disk_steps = [
                ('disk_processing', f'Processing disk {disk_num}/2: {disk_name}...', f'Disk {disk_name} configuration', 5),
                ('disk_creating', f'Creating disk {disk_name} (20G)...', 'Allocating disk space', 20),
                ('disk_created', f'Disk {disk_name} created successfully', 'Disk storage allocated', 40),
                ('disk_attaching', f'Attaching disk {disk_name} to VM...', f'Attaching disk {disk_name}', 50),
                ('disk_attached', f'Disk {disk_name} attached', 'Disk attached successfully', 60),
                ('disk_copying', f'Copying data for disk {disk_name}...', 'Transferring data to storage', 70),
                ('disk_detecting_type', 'Detected dir storage - direct transfer', 'File-based storage detected', 80),
                ('disk_downloading', f'Downloading vm-999-disk-{disk_num-1}.qcow2...', 'Download in progress', 85),
                ('disk_uploading', 'Uploading to destination...', 'Upload in progress', 90),
                ('disk_copied', f'Disk {disk_name} data copied successfully', 'Data transfer complete', 95),
            ]
            
            for step, message, detail, stage_progress in disk_steps:
                status = get_migration_status()
                if not status.get('active'):
                    break
                    
                update_migration_status(step, message=message, details=detail, stage_progress=stage_progress)
                migration_status['active'] = True  # Keep active flag
                time.sleep(0.5)  # Faster for disks
        
        # Final steps
        final_steps = [
            ('network_mapping', 'Applying network interface mappings...', 'Updating network configuration', 50),
            ('network_applied', 'Network mappings applied successfully', 'Network configured', 100),
            ('cleanup', 'Cleaning up temporary files...', 'Cleanup in progress', 70),
            ('completed', 'Migration completed successfully!', 'Migration finished', 100),
        ]
        
        for step, message, detail, stage_progress in final_steps:
            status = get_migration_status()
            if not status.get('active'):
                break
                
            update_migration_status(step, message=message, details=detail, stage_progress=stage_progress)
            migration_status['active'] = True  # Keep active flag
            time.sleep(0.8)
        
        migration_status['active'] = False
        
    except Exception as e:
        migration_status['active'] = False
        update_migration_status('error', message=f'Test migration failed: {str(e)}')
