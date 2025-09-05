from datetime import datetime

def format_size(size):
    """Format bytes to human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} PB"

def get_vm_info(proxmox, node, vmid):
    """Get VM information from Proxmox"""
    try:
        config = proxmox.nodes(node).qemu(vmid).config.get()
        status = proxmox.nodes(node).qemu(vmid).status.current.get()
        
        disks = []
        for key, value in config.items():
            if key.startswith('scsi') or key.startswith('virtio') or key.startswith('ide') or key.startswith('sata'):
                if 'disk' in str(value):
                    disks.append({
                        'device': key,
                        'storage': value.split(',')[0].split(':')[0],
                        'path': value
                    })
        
        return {
            'vmid': vmid,
            'name': config.get('name', f'VM-{vmid}'),
            'status': status.get('status', 'unknown'),
            'memory': config.get('memory', 0),
            'cores': config.get('cores', 0),
            'node': node,
            'disks': disks
        }
    except Exception as e:
        raise Exception(f"Failed to get VM info: {str(e)}")

def validate_migration_data(data):
    """Validate migration request data"""
    required_fields = ['source_cluster_id', 'dest_cluster_id', 'source_node', 'dest_node', 'vmid', 'storage_mappings']
    for field in required_fields:
        if field not in data:
            raise ValueError(f"Missing required field: {field}")
    
    # Validate storage mappings is not empty
    if not data.get('storage_mappings') or not isinstance(data['storage_mappings'], dict):
        raise ValueError("storage_mappings must be a non-empty dictionary")
    
    return True
