import paramiko
import os
import time
import logging
from utils import format_size

logger = logging.getLogger(__name__)

# Global variable to track migration status
migration_status = {
    'active': False,
    'vmid': None,
    'step': '',
    'progress': 0,
    'message': '',
    'details': [],
    'current_stage': '',
    'total_stages': 0,
    'current_disk': 0,
    'total_disks': 0,
    'disk_transfer': {
        'active': False,
        'current_disk_name': '',
        'transfer_type': '',  # 'download' or 'upload'
        'progress': 0,
        'speed': '',
        'eta': '',
        'transferred': 0,
        'total_size': 0
    }
}

# Migration stage definitions with fixed progress ranges
MIGRATION_STAGES = {
    'initializing': {'start': 0, 'end': 3, 'name': 'Инициализация'},
    'validation': {'start': 3, 'end': 6, 'name': 'Валидация данных'},
    'connecting': {'start': 6, 'end': 12, 'name': 'Подключение к источнику'},
    'vm_info': {'start': 12, 'end': 15, 'name': 'Получение информации о ВМ'},
    'vm_stopping': {'start': 15, 'end': 25, 'name': 'Остановка ВМ'},
    'ssh_connection': {'start': 25, 'end': 30, 'name': 'SSH подключение'},
    'dest_connecting': {'start': 30, 'end': 35, 'name': 'Подключение к назначению'},
    'config_reading': {'start': 35, 'end': 40, 'name': 'Чтение конфигурации'},
    'vm_creation': {'start': 40, 'end': 50, 'name': 'Создание ВМ'},
    'disk_migration': {'start': 50, 'end': 90, 'name': 'Миграция дисков'},
    'network_config': {'start': 90, 'end': 95, 'name': 'Настройка сети'},
    'cleanup': {'start': 95, 'end': 98, 'name': 'Очистка'},
    'completed': {'start': 98, 'end': 100, 'name': 'Завершение'}
}

def update_disk_transfer_progress(disk_name, transfer_type, progress, transferred_bytes=0, total_bytes=0, speed_mbps=0):
    """Update disk transfer progress for separate progress bar"""
    global migration_status
    
    migration_status['disk_transfer'] = {
        'active': True,
        'current_disk_name': disk_name,
        'transfer_type': transfer_type,  # 'download' or 'upload'
        'progress': min(progress, 100),
        'transferred': transferred_bytes,
        'total_size': total_bytes,
        'speed': format_speed(speed_mbps) if speed_mbps > 0 else '',
        'eta': calculate_eta(transferred_bytes, total_bytes, speed_mbps) if speed_mbps > 0 else ''
    }

def stop_disk_transfer_progress():
    """Stop disk transfer progress tracking"""
    global migration_status
    migration_status['disk_transfer']['active'] = False

def format_speed(speed_mbps):
    """Format transfer speed for display"""
    if speed_mbps >= 1000:
        return f"{speed_mbps/1000:.1f} GB/s"
    elif speed_mbps >= 1:
        return f"{speed_mbps:.1f} MB/s"
    else:
        return f"{speed_mbps*1000:.0f} KB/s"

def create_transfer_callback(disk_name, transfer_type, total_size):
    """Create callback function for SFTP transfer progress tracking"""
    start_time = time.time()
    last_update = 0
    
    def callback(transferred, total):
        nonlocal last_update
        current_time = time.time()
        
        # Update progress every 0.5 seconds to avoid too frequent updates
        if current_time - last_update >= 0.5:
            elapsed_time = current_time - start_time
            if elapsed_time > 0:
                speed_bps = transferred / elapsed_time
                speed_mbps = speed_bps / (1024 * 1024)
            else:
                speed_mbps = 0
            
            progress = (transferred / total * 100) if total > 0 else 0
            update_disk_transfer_progress(disk_name, transfer_type, progress, transferred, total, speed_mbps)
            last_update = current_time
    
    return callback

def calculate_eta(transferred, total, speed_mbps):
    """Calculate estimated time of arrival"""
    if speed_mbps == 0 or transferred >= total:
        return ""
    
    remaining_bytes = total - transferred
    remaining_mb = remaining_bytes / (1024 * 1024)
    eta_seconds = remaining_mb / speed_mbps if speed_mbps > 0 else 0
    
    if eta_seconds > 3600:
        hours = int(eta_seconds // 3600)
        minutes = int((eta_seconds % 3600) // 60)
        return f"{hours}h {minutes}m"
    elif eta_seconds > 60:
        minutes = int(eta_seconds // 60)
        seconds = int(eta_seconds % 60)
        return f"{minutes}m {seconds}s"
    else:
        return f"{int(eta_seconds)}s"

def calculate_stage_progress(stage_key, stage_progress=0):
    """Calculate overall progress based on current stage and stage progress"""
    if stage_key not in MIGRATION_STAGES:
        return 0
    
    # Special case for completed stage - always return 100%
    if stage_key == 'completed':
        return 100
    
    stage = MIGRATION_STAGES[stage_key]
    stage_range = stage['end'] - stage['start']
    stage_offset = (stage_range * stage_progress) / 100
    
    return min(100, stage['start'] + stage_offset)

def calculate_disk_progress(current_disk, total_disks, disk_stage_progress=0):
    """Calculate progress within disk migration stage"""
    if total_disks == 0:
        return 0
    
    # Each disk gets equal portion of the 40% allocated to disk migration (50-90%)
    disk_portion = 40 / total_disks
    completed_disks_progress = (current_disk - 1) * disk_portion
    current_disk_progress = (disk_stage_progress * disk_portion) / 100
    
    return completed_disks_progress + current_disk_progress

def update_migration_status(step, progress_override=None, message='', details=None, stage_progress=0):
    """Update global migration status with improved progress calculation"""
    global migration_status
    
    # Determine current stage from step
    current_stage = step
    if step in ['vm_stopped', 'vm_ready']:
        current_stage = 'vm_stopping'
    elif step in ['ssh_connecting', 'ssh_connected']:
        current_stage = 'ssh_connection'
    elif step in ['vm_id_check', 'vm_id_available', 'vm_id_changed', 'vm_creating', 'vm_created']:
        current_stage = 'vm_creation'
    elif step in ['disk_processing', 'disk_creating', 'disk_created', 'disk_attaching', 'disk_attached', 
                  'disk_copying', 'disk_copied', 'disk_detecting_type', 'disk_downloading', 'disk_uploading']:
        current_stage = 'disk_migration'
    elif step in ['network_mapping', 'network_applied']:
        current_stage = 'network_config'
    elif step in ['cleanup_done']:
        current_stage = 'cleanup'
    elif step == 'error':
        current_stage = 'error'
    
    # Calculate progress
    if progress_override is not None:
        calculated_progress = min(progress_override, 100)
    elif current_stage == 'disk_migration' and migration_status.get('total_disks', 0) > 0:
        # Special calculation for disk migration
        disk_progress = calculate_disk_progress(
            migration_status.get('current_disk', 1),
            migration_status.get('total_disks', 1),
            stage_progress
        )
        calculated_progress = 50 + disk_progress  # 50% is start of disk migration
    else:
        calculated_progress = calculate_stage_progress(current_stage, stage_progress)
    
    # Update status
    migration_status['step'] = step
    migration_status['current_stage'] = current_stage
    migration_status['progress'] = calculated_progress
    migration_status['message'] = message
    
    # Add to activity log with better formatting
    if details or message:
        timestamp = time.strftime("%H:%M:%S")
        
        # Create unique activity entry
        activity_text = details if details else message
        activity_entry = {
            'timestamp': timestamp,
            'message': activity_text,
            'stage': MIGRATION_STAGES.get(current_stage, {}).get('name', current_stage),
            'progress': int(calculated_progress),
            'key': f"{step}_{int(calculated_progress)}_{timestamp.replace(':', '')}"
        }
        
        # Prevent duplicate entries by checking recent entries
        recent_entries = migration_status['details'][-3:] if migration_status['details'] else []
        is_duplicate = any(
            entry.get('message') == activity_text and 
            abs(entry.get('progress', 0) - int(calculated_progress)) <= 1
            for entry in recent_entries
        )
        
        if not is_duplicate:
            migration_status['details'].append(activity_entry)
            
            # Limit to last 30 entries
            if len(migration_status['details']) > 30:
                migration_status['details'] = migration_status['details'][-30:]
    
    logger.warning(f"Migration Status: {current_stage} -> {step} - {calculated_progress:.1f}% - {message}")

def get_migration_status():
    """Get current migration status"""
    global migration_status
    return migration_status.copy()

def check_disk_space(ssh, path, required_size_gb):
    """Check if there's enough disk space at the specified path"""
    try:
        logger.warning(f"Checking disk space for path: {path}, required: {required_size_gb}GB")
        # Get disk usage for the path
        stdin, stdout, stderr = ssh.exec_command(f"df -BG '{path}' | tail -1")
        df_output = stdout.read().decode().strip()
        logger.warning(f"df output: {df_output}")
        
        if df_output:
            # Parse df output: Filesystem Size Used Avail Use% Mounted
            parts = df_output.split()
            if len(parts) >= 4:
                available_str = parts[3]  # Available space
                # Remove 'G' suffix and convert to int
                available_gb = int(available_str.rstrip('G'))
                logger.warning(f"Available space: {available_gb}GB, Required: {required_size_gb}GB")
                
                # Add 20% buffer
                required_with_buffer = int(required_size_gb * 1.2)
                
                if available_gb >= required_with_buffer:
                    logger.warning(f"Sufficient disk space available: {available_gb}GB >= {required_with_buffer}GB (with buffer)")
                    return True
                else:
                    logger.error(f"Insufficient disk space: {available_gb}GB < {required_with_buffer}GB (required with 20% buffer)")
                    return False
        
        logger.warning("Could not parse df output, assuming sufficient space")
        return True
        
    except Exception as e:
        logger.error(f"Error checking disk space: {str(e)}")
        return True  # If we can't check, proceed anyway

def copy_disk_data(source_cluster, dest_cluster, data, disk_file, ssh, sftp, base_progress=0, dest_storage=None):
    """Copy disk data from source to destination using local staging."""
    try:
        import uuid
        copy_id = str(uuid.uuid4())[:8]
        logger.warning(f"=== STARTING DISK COPY OPERATION [{copy_id}] ===")
        logger.warning(f"[{copy_id}] VM ID: {data.get('vmid', 'unknown')}")
        logger.warning(f"[{copy_id}] Disk file: {disk_file}")
        logger.warning(f"[{copy_id}] Destination storage: {dest_storage}")
        logger.warning(f"[{copy_id}] Base progress: {base_progress}")
        
        # Extract hostname from api_host (remove port if present)
        ssh_host = source_cluster.api_host.split(':')[0]
        
        # Determine source path and storage type
        storage_name = disk_file.split(':')[0] if ':' in disk_file else 'local-lvm'
        disk_path_with_options = disk_file.split(':')[1] if ':' in disk_file else disk_file
        disk_path = disk_path_with_options.split(',')[0]  # Remove options like size=10G
        
        # Extract just the filename from the disk path (remove any directory components)
        disk_name = disk_path.split('/')[-1]  # Get just the filename part
        
        logger.warning(f"[{copy_id}] Processing disk: {disk_file}")
        logger.warning(f"[{copy_id}] Storage: {storage_name}, Disk path: {disk_path}, Disk filename: {disk_name}")
        
        # Get storage information to determine storage type
        try:
            # Parse API host and token
            api_host = source_cluster.api_host
            host_part = api_host.split(':')[0]
            port = int(api_host.split(':')[1]) if ':' in api_host else 8006
            
            # Parse user and token
            user = source_cluster.api_user
            token_name = source_cluster.api_token_name
            
            from proxmoxer import ProxmoxAPI
            proxmox_source = ProxmoxAPI(
                host=host_part,
                port=port,
                user=user,
                token_name=token_name,
                token_value=source_cluster.api_token_secret,
                verify_ssl=False,
                timeout=30
            )
        except Exception as e:
            logger.error(f"Failed to connect to source Proxmox: {str(e)}")
            proxmox_source = None
        
        storage_info = None
        if proxmox_source:
            for node in proxmox_source.nodes.get():
                try:
                    for storage in proxmox_source.nodes(node['node']).storage.get():
                        if storage.get('storage') == storage_name:
                            storage_info = storage
                            break
                    if storage_info:
                        break
                except:
                    continue
        
        storage_type = storage_info.get('type', 'unknown') if storage_info else 'unknown'
        logger.warning(f"Detected storage type: {storage_type}")
        
        # Different logic based on storage type
        if storage_type in ['dir', 'glusterfs', 'nfs', 'cifs']:
            # File-based storage - direct file transfer without temp files
            logger.warning(f"[{copy_id}] Using file-based storage method for {storage_type}")
            return copy_file_based_storage(source_cluster, dest_cluster, data, storage_name, disk_name, disk_path, ssh, sftp, storage_type, base_progress, dest_storage, copy_id)
        else:
            # Block-based storage (LVM, ZFS, etc.) - use temp files
            logger.warning(f"[{copy_id}] Using block-based storage method for {storage_type}")
            return copy_block_based_storage(source_cluster, dest_cluster, data, storage_name, disk_name, ssh, sftp, base_progress, dest_storage, copy_id)
            
    except Exception as e:
        logger.error(f"=== ERROR IN DISK COPY OPERATION ===")
        logger.error(f"Error copying disk data: {str(e)}")
        logger.error(f"VM ID: {data.get('vmid', 'unknown')}")
        logger.error(f"Disk file: {disk_file}")
        raise

def copy_file_based_storage(source_cluster, dest_cluster, data, storage_name, disk_name, disk_path, ssh, sftp, storage_type, base_progress=0, dest_storage=None, copy_id="UNKNOWN"):
    """Copy disk files from file-based storage (dir, glusterfs, nfs) directly without temp files."""
    try:
        logger.warning(f"=== ENTERING copy_file_based_storage ===")
        logger.warning(f"Storage type: {storage_type}")
        logger.warning(f"Storage name: {storage_name}")
        logger.warning(f"Disk path: {disk_path}")
        logger.warning(f"Disk filename: {disk_name}")
        logger.warning(f"Destination storage: {dest_storage}")
        
        current_progress = base_progress
        update_migration_status('disk_detecting_type', message=f"Detected {storage_type} storage - using direct file transfer", 
                               details=f"Using direct file transfer method", stage_progress=75)
        logger.warning(f"Using direct file transfer for {storage_type} storage")
        
        # Common paths for different storage types
        storage_paths = {
            'dir': [f"/var/lib/vz/images", f"/var/lib/vz"],
            'glusterfs': [f"/mnt/pve/{storage_name}", f"/var/lib/vz"],
            'nfs': [f"/mnt/pve/{storage_name}", f"/var/lib/vz"],
            'cifs': [f"/mnt/pve/{storage_name}", f"/var/lib/vz"]
        }
        
        search_paths = storage_paths.get(storage_type, [f"/var/lib/vz/images", f"/var/lib/vz"])
        
        update_migration_status('disk_locating', message=f"Locating disk file on {storage_type} storage...", 
                               details=f"Determining disk file path", stage_progress=78)
        
        # Try to find the disk file
        source_path = None
        for base_path in search_paths:
            # Look for the disk file in various possible locations
            # Use the original disk_path for searching, but disk_name for destination
            possible_paths = [
                f"{base_path}/images/{disk_path}",                # Most common: images/original/path/diskfile
                f"{base_path}/{disk_path}",                       # Alternative: original/path/diskfile
                f"{base_path}/images/{data['vmid']}/{disk_name}",  # Standard: images/vmid/diskfile
                f"{base_path}/{data['vmid']}/{disk_name}",         # Alternative: vmid/diskfile
                f"{base_path}/{disk_name}",                       # Direct: diskfile
                # Try with different extensions if original has none
                f"{base_path}/images/{disk_path}.qcow2",
                f"{base_path}/images/{disk_path}.raw",
                f"{base_path}/images/{data['vmid']}/{disk_name}.qcow2",
                f"{base_path}/images/{data['vmid']}/{disk_name}.raw",
                f"{base_path}/{data['vmid']}/{disk_name}.qcow2",
                f"{base_path}/{data['vmid']}/{disk_name}.raw"
            ]
            
            logger.warning(f"Searching in base path: {base_path}")
            logger.warning(f"Looking for disk path: {disk_path} (filename: {disk_name})")
            
            for path in possible_paths:
                logger.warning(f"Checking path: {path}")
                stdin, stdout, stderr = ssh.exec_command(f"test -f {path} && echo 'FOUND' || echo 'NOT_FOUND'")
                result = stdout.read().decode().strip()
                if result == 'FOUND':
                    source_path = path
                    update_migration_status('disk_found', message=f"Disk file found", 
                                           details=f"Found at: {source_path}", stage_progress=80)
                    logger.warning(f"Found disk file at: {source_path}")
                    break
            
            if source_path:
                break
        
        if not source_path:
            # Fallback: search with find command
            update_migration_status('disk_searching', message=f"Searching for disk file with find command...", 
                                   details=f"Performing extended search", stage_progress=79)
            logger.warning(f"File not found in standard locations, searching with find...")
            
            # Extract just the filename from disk_name (in case it has path components)
            disk_filename = disk_name.split('/')[-1]  # Get just the filename part
            
            # Search for the exact filename
            stdin, stdout, stderr = ssh.exec_command(f"find /var/lib/vz /mnt/pve -name '{disk_filename}' -type f 2>/dev/null")
            find_output = stdout.read().decode().strip()
            
            if find_output:
                logger.warning(f"Found potential files: {find_output}")
                for found_file in find_output.split('\n'):
                    if found_file and found_file.strip():
                        source_path = found_file.strip()
                        update_migration_status('disk_found_fallback', message=f"Disk file found", 
                                               details=f"Found: {source_path}", stage_progress=82)
                        logger.warning(f"Using found file: {source_path}")
                        break
            else:
                # If exact name not found, try wildcard search
                logger.warning(f"Exact filename not found, trying wildcard search...")
                stdin, stdout, stderr = ssh.exec_command(f"find /var/lib/vz /mnt/pve -name '*{disk_filename}*' -type f 2>/dev/null | head -5")
                find_output = stdout.read().decode().strip()
                
                if find_output:
                    logger.warning(f"Found potential files with wildcard: {find_output}")
                    for found_file in find_output.split('\n'):
                        if found_file and disk_filename in found_file:
                            source_path = found_file.strip()
                            update_migration_status('disk_found_fallback', message=f"Disk file found", 
                                                   details=f"Found: {source_path}", stage_progress=82)
                            logger.warning(f"Using found file: {source_path}")
                            break
        
        if not source_path:
            raise Exception(f"Could not locate source disk file {disk_name} on {storage_type} storage")
        
        logger.warning(f"Source file path: {source_path}")
        
        # Get file size for verification and space checking
        stdin, stdout, stderr = ssh.exec_command(f"ls -la {source_path}")
        file_info = stdout.read().decode().strip()
        update_migration_status('disk_size_check', message=f"Checking file size...", 
                               details=f"Determining size for copy operation", stage_progress=83)
        logger.warning(f"[{copy_id}] Source file info: {file_info}")
        
        # Extract file size from ls output
        source_file_size_bytes = 0
        try:
            parts = file_info.split()
            if len(parts) >= 5:
                source_file_size_bytes = int(parts[4])  # Size in bytes
                source_file_size_gb = source_file_size_bytes / (1024**3)  # Convert to GB
                logger.warning(f"[{copy_id}] Source file size: {source_file_size_bytes} bytes ({source_file_size_gb:.2f} GB)")
        except Exception as e:
            logger.warning(f"[{copy_id}] Could not parse file size from: {file_info}, error: {e}")
            source_file_size_gb = 10  # Default fallback size
        
        # Determine destination path based on destination storage
        # Extract just the filename from disk_name (remove any path components)
        disk_filename = os.path.basename(disk_name)
        logger.warning(f"[{copy_id}] Extracted disk filename: {disk_filename}")
        
        dest_path = f"/var/lib/vz/images/{data['vmid']}/{disk_filename}.raw"  # Default path
        dest_dir = os.path.dirname(dest_path)
        
        logger.warning(f"[{copy_id}] Initial destination path variables:")
        logger.warning(f"[{copy_id}]   VM ID: {data['vmid']}")
        logger.warning(f"[{copy_id}]   Disk path: {disk_path}")
        logger.warning(f"[{copy_id}]   Disk filename: {disk_filename}")
        logger.warning(f"[{copy_id}]   Default dest_path: {dest_path}")
        logger.warning(f"[{copy_id}]   Default dest_dir: {dest_dir}")
        logger.warning(f"[{copy_id}]   Destination storage: {dest_storage}")
        
        # If dest_storage is provided, get storage information and determine correct path
        if dest_storage:
            logger.warning(f"[{copy_id}] Determining path for destination storage: {dest_storage}")
            try:
                # Connect to destination Proxmox to get storage info
                dest_host = dest_cluster.api_host.split(':')[0]
                port = int(dest_cluster.api_host.split(':')[1]) if ':' in dest_cluster.api_host else 8006
                
                user_token = dest_cluster.api_token_id.split('!')
                user = user_token[0]
                token_name = user_token[1] if len(user_token) > 1 else 'default'
                
                from proxmoxer import ProxmoxAPI
                dest_proxmox = ProxmoxAPI(
                    host=dest_host,
                    port=port,
                    user=user,
                    token_name=token_name,
                    token_value=dest_cluster.api_token_secret,
                    verify_ssl=False,
                    timeout=30
                )
                
                logger.warning(f"[{copy_id}] Connected to destination Proxmox, getting storage info...")
                
                # Get destination storage information
                dest_storage_info = None
                for storage in dest_proxmox.nodes(data['dest_node']).storage.get():
                    logger.warning(f"[{copy_id}] Found storage: {storage.get('storage')} (type: {storage.get('type')})")
                    if storage.get('storage') == dest_storage:
                        dest_storage_info = storage
                        logger.warning(f"[{copy_id}] Matched destination storage: {dest_storage_info}")
                        break
                
                if dest_storage_info:
                    dest_storage_type = dest_storage_info.get('type', 'dir')
                    logger.warning(f"[{copy_id}] Destination storage '{dest_storage}' type: {dest_storage_type}")
                    
                    # Determine correct destination path based on storage type
                    if dest_storage_type in ['dir', 'glusterfs', 'nfs', 'cifs']:
                        # For file-based storage, files go directly to the mount point
                        # Note: Path should be /mnt/pve/{storage}/images/{vmid}/{filename}
                        # Files should be placed directly in the vmid directory, not in a subdirectory
                        if dest_storage_type == 'dir':
                            # For 'dir' storage, check if it has a specific path or use mount point
                            content_path = dest_storage_info.get('path', '')
                            logger.warning(f"[{copy_id}] Dir storage content_path from API: '{content_path}'")
                            
                            # If the content_path is standard /var/lib/vz, use mount point instead
                            if not content_path or content_path in ['/var/lib/vz', '/var/lib/vz/']:
                                # Use mount point for dir storage - file goes directly in vmid directory
                                dest_path = f"/mnt/pve/{dest_storage}/images/{data['vmid']}/{disk_filename}.raw"
                                dest_dir = f"/mnt/pve/{dest_storage}/images/{data['vmid']}"
                                logger.warning(f"[{copy_id}] Using mount point for dir storage: {dest_path}")
                            else:
                                # Use the specific path from API - file goes directly in vmid directory
                                dest_path = f"{content_path}/images/{data['vmid']}/{disk_filename}.raw"
                                dest_dir = f"{content_path}/images/{data['vmid']}"
                                logger.warning(f"[{copy_id}] Using specific path for dir storage: {dest_path}")
                        else:
                            # For mounted storage (glusterfs, nfs, cifs), use mount point - file goes directly in vmid directory
                            dest_path = f"/mnt/pve/{dest_storage}/images/{data['vmid']}/{disk_filename}.raw"
                            dest_dir = f"/mnt/pve/{dest_storage}/images/{data['vmid']}"
                            logger.warning(f"[{copy_id}] Mounted storage path: {dest_path}")
                        
                        logger.warning(f"[{copy_id}] Using destination storage path: {dest_path}")
                        logger.warning(f"[{copy_id}] Destination directory: {dest_dir}")
                    else:
                        # For block storage, still use default path as Proxmox handles the mapping
                        logger.warning(f"[{copy_id}] Block storage detected, using default path: {dest_path}")
                else:
                    logger.warning(f"[{copy_id}] No storage info found for '{dest_storage}', using default path")
                        
            except Exception as e:
                logger.warning(f"[{copy_id}] Could not get destination storage info, using default path: {str(e)}")
                # Fall back to default path
                dest_path = f"/var/lib/vz/images/{data['vmid']}/{disk_filename}.raw"
                dest_dir = os.path.dirname(dest_path)
        else:
            logger.warning(f"[{copy_id}] No dest_storage provided, using default path: {dest_path}")
        
        # Log final calculated paths
        logger.warning(f"[{copy_id}] FINAL CALCULATED PATHS:")
        logger.warning(f"[{copy_id}]   Final dest_path: {dest_path}")
        logger.warning(f"[{copy_id}]   Final dest_dir: {dest_dir}")
        logger.warning(f"[{copy_id}]   dest_host: {dest_host}")
        
        # Check available disk space on destination before proceeding
        logger.warning(f"[{copy_id}] Checking disk space on destination server...")
        dest_ssh = paramiko.SSHClient()
        dest_ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        dest_host = dest_cluster.api_host.split(':')[0]
        dest_ssh.connect(dest_host, username='root', password=dest_cluster.ssh_password)
        
        # Check disk space for destination directory
        if not check_disk_space(dest_ssh, dest_dir, source_file_size_gb):
            dest_ssh.close()
            raise Exception(f"Insufficient disk space on destination server for path {dest_dir}. Required: {source_file_size_gb:.2f}GB")
        
        # Create destination directory
        update_migration_status('disk_dest_prep', current_progress + 6, f"Preparing destination directory...")
        logger.warning(f"[{copy_id}] Creating destination directory: {dest_dir}")
        stdin, stdout, stderr = dest_ssh.exec_command(f"mkdir -p {dest_dir}")
        create_result = stderr.read().decode().strip()
        if create_result:
            logger.warning(f"[{copy_id}] Directory creation output: {create_result}")
        
        # Verify directory was created successfully
        stdin, stdout, stderr = dest_ssh.exec_command(f"ls -ld {dest_dir}")
        verify_result = stdout.read().decode().strip()
        verify_error = stderr.read().decode().strip()
        logger.warning(f"[{copy_id}] Directory verification: {verify_result}")
        if verify_error:
            logger.error(f"[{copy_id}] Directory verification error: {verify_error}")
            dest_ssh.close()
            raise Exception(f"Failed to create or verify destination directory {dest_dir}: {verify_error}")
        
        dest_ssh.close()
        
        # Log the final destination path for debugging
        logger.warning(f"[{copy_id}] Final destination path: {dest_path}")
        logger.warning(f"[{copy_id}] Final destination directory: {dest_dir}")
        
        # Direct file transfer via local staging
        update_migration_status('disk_download_start', current_progress + 7, f"Starting file download from source...")
        logger.warning(f"Starting direct file transfer")
        
        # Create local temporary directory
        local_temp_dir = "temp_migration"
        if not os.path.exists(local_temp_dir):
            os.makedirs(local_temp_dir)
        
        # Use original filename with extension
        source_filename = os.path.basename(source_path)
        local_temp_file = os.path.join(local_temp_dir, f"vm-{data['vmid']}-{source_filename}")
        logger.warning(f"Local temp file: {local_temp_file}")
        
        # Download file from source server to local machine
        update_migration_status('disk_downloading', message=f"Downloading {source_filename}...", 
                               details=f"Transferring file from source", stage_progress=85)
        logger.warning(f"Downloading {source_path} to {local_temp_file}")
        
        # Get file size for progress tracking
        file_size = sftp.stat(source_path).st_size
        update_disk_transfer_progress(source_filename, 'download', 0, 0, file_size, 0)
        
        # Create callback for progress tracking
        download_callback = create_transfer_callback(source_filename, 'download', file_size)
        sftp.get(source_path, local_temp_file, callback=download_callback)
        
        # Stop disk transfer progress
        stop_disk_transfer_progress()
        
        # Verify local file size
        local_size = os.path.getsize(local_temp_file)
        update_migration_status('disk_download_complete', current_progress + 12, f"Download complete ({format_size(local_size)})")
        logger.warning(f"Local file size: {local_size} bytes")
        
        # Upload file from local machine to destination server
        update_migration_status('disk_upload_start', current_progress + 13, f"Starting upload to destination...")
        logger.warning(f"Connecting to destination server for upload")
        dest_transport = paramiko.Transport((dest_host, 22))
        dest_transport.connect(username='root', password=dest_cluster.ssh_password)
        dest_sftp = paramiko.SFTPClient.from_transport(dest_transport)
        
        # Verify destination directory exists via SFTP
        logger.warning(f"[{copy_id}] Verifying destination directory via SFTP: {dest_dir}")
        try:
            dest_sftp.stat(dest_dir)
            logger.warning(f"[{copy_id}] Destination directory confirmed to exist via SFTP")
        except FileNotFoundError:
            logger.error(f"[{copy_id}] Destination directory does not exist via SFTP: {dest_dir}")
            # Try to create it via SFTP
            try:
                dest_sftp.mkdir(dest_dir)
                logger.warning(f"[{copy_id}] Created destination directory via SFTP: {dest_dir}")
            except Exception as mkdir_error:
                logger.error(f"[{copy_id}] Failed to create directory via SFTP: {mkdir_error}")
                dest_sftp.close()
                dest_transport.close()
                raise Exception(f"Cannot create destination directory {dest_dir}: {mkdir_error}")
        
        update_migration_status('disk_uploading', message=f"Uploading to destination server...", 
                               details=f"Transferring file to destination", stage_progress=90)
        logger.warning(f"Uploading {local_temp_file} to {dest_path}")
        
        # Start upload progress tracking
        upload_size = os.path.getsize(local_temp_file)
        update_disk_transfer_progress(source_filename, 'upload', 0, 0, upload_size, 0)
        
        # Create callback for upload progress tracking
        upload_callback = create_transfer_callback(source_filename, 'upload', upload_size)
        
        try:
            dest_sftp.put(local_temp_file, dest_path, callback=upload_callback)
            
            # Stop disk transfer progress
            stop_disk_transfer_progress()
            
            logger.warning(f"[{copy_id}] Upload successful")
            
            # Rename file to match the new VM ID format
            # From: vm-100-disk-0.qcow2.raw -> To: vm-104-disk-0.qcow2
            original_filename = os.path.basename(dest_path)
            logger.warning(f"[{copy_id}] Starting file rename process...")
            logger.warning(f"[{copy_id}] Original filename: {original_filename}")
            logger.warning(f"[{copy_id}] Target VM ID from data: {data.get('vmid', 'unknown')}")
            
            # Extract disk number from original filename
            if 'disk-' in original_filename:
                parts = original_filename.split('-')
                disk_part = None
                disk_number = None
                
                for i, part in enumerate(parts):
                    if part.startswith('disk') and i + 1 < len(parts):
                        disk_part = part
                        # Get the number after disk- (remove any extensions)
                        disk_number_with_ext = parts[i + 1]
                        disk_number = disk_number_with_ext.replace('.raw', '').replace('.qcow2', '')
                        break
                
                if disk_part and disk_number:
                    # Create new filename with target VM ID
                    new_filename = f"vm-{data['vmid']}-{disk_part}-{disk_number}.qcow2"
                    # Use Unix-style path separator for remote server
                    new_dest_path = f"{os.path.dirname(dest_path).replace(chr(92), '/')}/{new_filename}"
                    
                    logger.warning(f"[{copy_id}] Renaming file from {original_filename} to {new_filename}")
                    
                    # Use SFTP to rename the file
                    try:
                        dest_sftp.rename(dest_path, new_dest_path)
                        logger.warning(f"[{copy_id}] Successfully renamed file to {new_dest_path}")
                        dest_path = new_dest_path  # Update dest_path for return value
                    except Exception as rename_error:
                        logger.warning(f"[{copy_id}] Could not rename file via SFTP: {rename_error}")
                        # Try using SSH command as fallback
                        try:
                            dest_sftp.close()
                            dest_transport.close()
                            
                            # Use SSH to rename
                            dest_ssh = paramiko.SSHClient()
                            dest_ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                            dest_host = dest_cluster.api_host.split(':')[0]
                            dest_ssh.connect(dest_host, username='root', password=dest_cluster.ssh_password)
                            
                            rename_command = f"mv '{dest_path}' '{new_dest_path}'"
                            logger.warning(f"[{copy_id}] Executing rename command: {rename_command}")
                            stdin, stdout, stderr = dest_ssh.exec_command(rename_command)
                            rename_result = stderr.read().decode().strip()
                            
                            if rename_result:
                                logger.warning(f"[{copy_id}] Rename command error: {rename_result}")
                            else:
                                logger.warning(f"[{copy_id}] Successfully renamed file via SSH to {new_dest_path}")
                                dest_path = new_dest_path
                            
                            dest_ssh.close()
                            # Re-establish SFTP connection for cleanup
                            dest_transport = paramiko.Transport((dest_host, 22))
                            dest_transport.connect(username='root', password=dest_cluster.ssh_password)
                            dest_sftp = paramiko.SFTPClient.from_transport(dest_transport)
                            
                        except Exception as ssh_rename_error:
                            logger.error(f"[{copy_id}] Failed to rename file via SSH: {ssh_rename_error}")
                            # Continue without renaming - file is uploaded but with wrong name
                else:
                    logger.warning(f"[{copy_id}] Could not parse disk information from filename: {original_filename}")
            else:
                logger.warning(f"[{copy_id}] Filename does not contain 'disk-' pattern: {original_filename}")
                
        except Exception as upload_error:
            logger.error(f"[{copy_id}] Upload failed: {upload_error}")
            dest_sftp.close()
            dest_transport.close()
            raise Exception(f"Failed to upload file to {dest_path}: {upload_error}")
        
        dest_sftp.close()
        dest_transport.close()
        
        update_migration_status('disk_upload_complete', current_progress + 18, f"Upload complete")
        logger.warning(f"Successfully uploaded to destination server")
        
        # Clean up local temp file
        update_migration_status('disk_cleanup', current_progress + 19, f"Cleaning up temporary files...")
        logger.warning(f"Cleaning up local temporary file")
        for attempt in range(3):
            try:
                time.sleep(1)
                os.remove(local_temp_file)
                logger.warning(f"Cleaned up local temporary file: {local_temp_file}")
                break
            except PermissionError:
                if attempt == 2:
                    logger.warning(f"Could not remove local temp file immediately: {local_temp_file}")
                else:
                    logger.warning(f"Retrying to remove local temp file: attempt {attempt + 1}")
        
        logger.warning(f"Successfully copied disk data to {dest_path}")
        logger.warning(f"=== EXITING copy_file_based_storage SUCCESSFULLY ===")
        return dest_path
        
    except Exception as e:
        logger.error(f"=== ERROR IN copy_file_based_storage ===")
        logger.error(f"Error details: {str(e)}")
        logger.error(f"Storage type: {storage_type}")
        logger.error(f"Disk name: {disk_name}")
        logger.error(f"Destination storage: {dest_storage}")
        
        # Clean up local temp file if it exists
        try:
            if 'local_temp_file' in locals() and os.path.exists(local_temp_file):
                os.remove(local_temp_file)
                logger.warning(f"Cleaned up local temp file: {local_temp_file}")
        except:
            pass
        raise Exception(f"Failed to copy file-based storage: {str(e)}")

def copy_block_based_storage(source_cluster, dest_cluster, data, storage_name, disk_name, ssh, sftp, base_progress=0, dest_storage=None, copy_id="UNKNOWN"):
    """Copy disk data from block-based storage (LVM, ZFS) using temporary files."""
    try:
        logger.warning(f"=== ENTERING copy_block_based_storage ===")
        logger.warning(f"Storage name: {storage_name}")
        logger.warning(f"Disk name: {disk_name}")
        logger.warning(f"Destination storage: {dest_storage}")
        
        # Extract just the filename from disk_name (remove any path components)
        disk_filename = os.path.basename(disk_name)
        logger.warning(f"Extracted disk filename: {disk_filename}")
        
        current_progress = base_progress
        update_migration_status('disk_detecting_block', current_progress + 1, f"Detected block-based storage - using temporary file method")
        logger.warning(f"Using temporary file method for block-based storage")
        
        update_migration_status('disk_locating_block', current_progress + 2, f"Locating block device...")
        
        # Try to find the actual file on the source system
        stdin, stdout, stderr = ssh.exec_command(f"find /var/lib/vz /dev -name '*{disk_name}*' 2>/dev/null | head -5")
        exit_status = stdout.channel.recv_exit_status()
        find_output = stdout.read().decode().strip()
        
        source_path = None
        logger.warning(f"Find command exit status: {exit_status}")
        if find_output:
            logger.warning(f"Found potential files: {find_output}")
            for found_file in find_output.split('\n'):
                if found_file and ('disk' in found_file or 'vm-' in found_file):
                    source_path = found_file.strip()
                    update_migration_status('disk_found_block', current_progress + 3, f"Found block device: {source_path}")
                    logger.warning(f"Using found file: {source_path}")
                    break
        
        if not source_path:
            raise Exception(f"Could not locate source disk file for {disk_name}")
        
        logger.warning(f"Final source path: {source_path}")
        
        # Check if this is a block device (LVM) or regular file
        stdin, stdout, stderr = ssh.exec_command(f"test -b {source_path} && echo 'BLOCK' || echo 'FILE'")
        device_type = stdout.read().decode().strip()
        update_migration_status('disk_type_detected', current_progress + 4, f"Device type: {device_type}")
        logger.warning(f"Device type: {device_type}")
        
        if device_type == 'BLOCK':
            # This is a block device (LVM), we need to create a temporary file first
            update_migration_status('disk_temp_create', current_progress + 5, f"Creating temporary file from block device...")
            logger.warning(f"Creating temporary file from block device")
            
            # Create temporary file on source server - use /var/tmp instead of /tmp
            clean_disk_name = disk_name.replace('/', '-').replace('\\', '-').replace(':', '-')
            temp_filename = f"vm-{data['vmid']}-{clean_disk_name}.img"
            temp_file = f"/var/tmp/{temp_filename}"
            
            # Ensure /var/tmp directory exists
            ssh.exec_command("mkdir -p /var/tmp")
            
            logger.warning(f"Temp file path: {temp_file}")
            
            # Use dd to create a file from the block device
            dd_command = f"dd if={source_path} of={temp_file} bs=1M"
            update_migration_status('disk_dd_start', current_progress + 6, f"Starting dd command to create temporary file...")
            logger.warning(f"Creating temp file: {dd_command}")
            
            stdin, stdout, stderr = ssh.exec_command(dd_command)
            exit_status = stdout.channel.recv_exit_status()
            dd_output = stdout.read().decode()
            dd_error = stderr.read().decode()
            
            logger.warning(f"DD command exit status: {exit_status}")
            logger.warning(f"DD output: {dd_output}")
            if dd_error:
                logger.warning(f"DD error: {dd_error}")
            
            if exit_status != 0:
                raise Exception(f"Failed to create temporary file from block device: {dd_error}")
            
            # Verify the temporary file was created
            stdin, stdout, stderr = ssh.exec_command(f"ls -la {temp_file}")
            ls_output = stdout.read().decode()
            update_migration_status('disk_temp_created', current_progress + 10, f"Temporary file created successfully", f"File info: {ls_output}")
            logger.warning(f"Temp file verification: {ls_output}")
            
            try:
                update_migration_status('disk_transfer_start', current_progress + 11, f"Starting file transfer via local staging...")
                logger.warning(f"Starting file transfer via local staging")
                
                # Create local temporary directory
                local_temp_dir = "temp_migration"
                if not os.path.exists(local_temp_dir):
                    os.makedirs(local_temp_dir)
                
                local_temp_file = os.path.join(local_temp_dir, temp_filename)
                logger.warning(f"Local temp file: {local_temp_file}")
                
                # Download file from source server to local machine
                update_migration_status('disk_downloading_block', current_progress + 12, f"Downloading temporary file...")
                logger.warning(f"Downloading {temp_file} to {local_temp_file}")
                
                # Get file size for progress tracking
                temp_file_size = sftp.stat(temp_file).st_size
                update_disk_transfer_progress(temp_filename, 'download', 0, 0, temp_file_size, 0)
                
                # Create callback for progress tracking
                download_callback = create_transfer_callback(temp_filename, 'download', temp_file_size)
                sftp.get(temp_file, local_temp_file, callback=download_callback)
                
                # Stop disk transfer progress
                stop_disk_transfer_progress()
                
                # Verify local file size
                local_size = os.path.getsize(local_temp_file)
                update_migration_status('disk_download_complete_block', current_progress + 16, f"Download complete ({format_size(local_size)})")
                logger.warning(f"Local file size: {local_size} bytes")
                
                # Determine destination path on target storage
                # Extract just the filename from disk_name
                disk_filename = os.path.basename(disk_name)
                dest_path = f"/var/lib/vz/images/{data['vmid']}/{disk_filename}.raw"  # Default path
                dest_dir = os.path.dirname(dest_path)
                
                # If dest_storage is provided, adjust path accordingly
                if dest_storage:
                    logger.warning(f"[{copy_id}] Determining path for destination storage: {dest_storage}")
                    try:
                        # Connect to destination Proxmox to get storage info
                        dest_host = dest_cluster.api_host.split(':')[0]
                        port = int(dest_cluster.api_host.split(':')[1]) if ':' in dest_cluster.api_host else 8006
                        
                        user_token = dest_cluster.api_token_id.split('!')
                        user = user_token[0]
                        token_name = user_token[1] if len(user_token) > 1 else 'default'
                        
                        from proxmoxer import ProxmoxAPI
                        dest_proxmox = ProxmoxAPI(
                            host=dest_host,
                            port=port,
                            user=user,
                            token_name=token_name,
                            token_value=dest_cluster.api_token_secret,
                            verify_ssl=False,
                            timeout=30
                        )
                        
                        # Get destination storage information
                        dest_storage_info = None
                        for storage in dest_proxmox.nodes(data['dest_node']).storage.get():
                            if storage.get('storage') == dest_storage:
                                dest_storage_info = storage
                                break
                        
                        if dest_storage_info:
                            dest_storage_type = dest_storage_info.get('type', 'dir')
                            logger.warning(f"[{copy_id}] Destination storage '{dest_storage}' type: {dest_storage_type}")
                            
                            if dest_storage_type in ['dir', 'glusterfs', 'nfs', 'cifs']:
                                if dest_storage_type == 'dir':
                                    content_path = dest_storage_info.get('path', '')
                                    if not content_path or content_path in ['/var/lib/vz', '/var/lib/vz/']:
                                        dest_path = f"/mnt/pve/{dest_storage}/images/{data['vmid']}/{disk_filename}.raw"
                                        dest_dir = f"/mnt/pve/{dest_storage}/images/{data['vmid']}"
                                    else:
                                        dest_path = f"{content_path}/images/{data['vmid']}/{disk_filename}.raw"
                                        dest_dir = f"{content_path}/images/{data['vmid']}"
                                else:
                                    dest_path = f"/mnt/pve/{dest_storage}/images/{data['vmid']}/{disk_filename}.raw"
                                    dest_dir = f"/mnt/pve/{dest_storage}/images/{data['vmid']}"
                                
                                logger.warning(f"[{copy_id}] Using destination storage path: {dest_path}")
                    except Exception as e:
                        logger.warning(f"[{copy_id}] Could not get destination storage info, using default path: {str(e)}")
                else:
                    logger.warning(f"[{copy_id}] No dest_storage provided, using default path: {dest_path}")
                
                # Create destination directory
                update_migration_status('disk_dest_prep_block', current_progress + 17, f"Preparing destination directory...")
                dest_ssh = paramiko.SSHClient()
                dest_ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                dest_host = dest_cluster.api_host.split(':')[0]
                
                dest_ssh.connect(dest_host, username='root', password=dest_cluster.ssh_password)
                stdin, stdout, stderr = dest_ssh.exec_command(f"mkdir -p {dest_dir}")
                dest_ssh.close()
                
                # Upload file from local machine to destination server
                update_migration_status('disk_upload_start_block', current_progress + 18, f"Uploading to destination server...")
                logger.warning(f"Connecting to destination server for upload")
                dest_transport = paramiko.Transport((dest_host, 22))
                dest_transport.connect(username='root', password=dest_cluster.ssh_password)
                dest_sftp = paramiko.SFTPClient.from_transport(dest_transport)
                
                logger.warning(f"Uploading {local_temp_file} to {dest_path}")
                
                # Start upload progress tracking
                upload_size = os.path.getsize(local_temp_file)
                update_disk_transfer_progress(temp_filename, 'upload', 0, 0, upload_size, 0)
                
                # Create callback for upload progress tracking
                upload_callback = create_transfer_callback(temp_filename, 'upload', upload_size)
                dest_sftp.put(local_temp_file, dest_path, callback=upload_callback)
                
                # Stop disk transfer progress
                stop_disk_transfer_progress()
                
                # Rename file to match the new VM ID format (same logic as file-based storage)
                original_filename = os.path.basename(dest_path)
                logger.warning(f"[{copy_id}] Starting block storage file rename process...")
                logger.warning(f"[{copy_id}] Original filename: {original_filename}")
                
                if 'disk-' in original_filename:
                    parts = original_filename.split('-')
                    disk_part = None
                    disk_number = None
                    
                    for i, part in enumerate(parts):
                        if part.startswith('disk') and i + 1 < len(parts):
                            disk_part = part
                            disk_number_with_ext = parts[i + 1]
                            disk_number = disk_number_with_ext.replace('.raw', '').replace('.qcow2', '')
                            break
                    
                    if disk_part and disk_number:
                        new_filename = f"vm-{data['vmid']}-{disk_part}-{disk_number}.qcow2"
                        # Use Unix-style path separator for remote server
                        new_dest_path = f"{os.path.dirname(dest_path).replace(chr(92), '/')}/{new_filename}"
                        
                        logger.warning(f"[{copy_id}] Renaming block storage file from {original_filename} to {new_filename}")
                        
                        try:
                            dest_sftp.rename(dest_path, new_dest_path)
                            logger.warning(f"[{copy_id}] Successfully renamed block file to {new_dest_path}")
                            dest_path = new_dest_path
                        except Exception as rename_error:
                            logger.warning(f"[{copy_id}] SFTP rename failed, trying SSH: {rename_error}")
                            # Close SFTP and use SSH for rename
                            dest_sftp.close()
                            dest_transport.close()
                            
                            dest_ssh = paramiko.SSHClient()
                            dest_ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                            dest_ssh.connect(dest_host, username='root', password=dest_cluster.ssh_password)
                            
                            rename_command = f"mv '{dest_path}' '{new_dest_path}'"
                            logger.warning(f"[{copy_id}] Block storage rename command: {rename_command}")
                            stdin, stdout, stderr = dest_ssh.exec_command(rename_command)
                            rename_result = stderr.read().decode().strip()
                            
                            if not rename_result:
                                logger.warning(f"[{copy_id}] Successfully renamed block file via SSH")
                                dest_path = new_dest_path
                            else:
                                logger.warning(f"[{copy_id}] SSH rename result: {rename_result}")
                            
                            dest_ssh.close()
                            # Re-establish connections for cleanup
                            dest_transport = paramiko.Transport((dest_host, 22))
                            dest_transport.connect(username='root', password=dest_cluster.ssh_password)
                            dest_sftp = paramiko.SFTPClient.from_transport(dest_transport)
                
                dest_sftp.close()
                dest_transport.close()
                update_migration_status('disk_upload_complete_block', current_progress + 22, f"Upload complete")
                logger.warning(f"Successfully uploaded to destination server")
                
                # Clean up temporary files
                update_migration_status('disk_cleanup_block', current_progress + 23, f"Cleaning up temporary files...")
                logger.warning(f"Cleaning up temporary files")
                
                # Remove remote temp file
                stdin, stdout, stderr = ssh.exec_command(f"rm -f {temp_file}")
                logger.warning(f"Cleaned up remote temporary file: {temp_file}")
                
                # Remove local temp file (with retry in case file is still locked)
                for attempt in range(3):
                    try:
                        time.sleep(1)  # Small delay to ensure file is released
                        os.remove(local_temp_file)
                        logger.warning(f"Cleaned up local temporary file: {local_temp_file}")
                        break
                    except PermissionError:
                        if attempt == 2:  # Last attempt
                            logger.warning(f"Could not remove local temp file immediately: {local_temp_file} (will be cleaned up later)")
                        else:
                            logger.warning(f"Retrying to remove local temp file: attempt {attempt + 1}")
                
                return dest_path
                
            except Exception as e:
                # Clean up temporary files even if transfer failed
                try:
                    stdin, stdout, stderr = ssh.exec_command(f"rm -f {temp_file}")
                    if 'local_temp_file' in locals() and os.path.exists(local_temp_file):
                        os.remove(local_temp_file)
                except:
                    pass
                raise Exception(f"Failed to transfer temporary file: {str(e)}")
        else:
            raise Exception("Only block device copying is currently supported for this storage type")
            
    except Exception as e:
        logger.error(f"Error copying block-based storage: {str(e)}")
        raise
