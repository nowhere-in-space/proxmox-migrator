from proxmoxer import ProxmoxAPI
import requests
import logging

logger = logging.getLogger(__name__)

def connect_to_proxmox(cluster):
    """Connect to Proxmox cluster using API credentials"""
    try:
        # Parse API host and port
        api_host = cluster.api_host.strip()  # Remove any whitespace
        logger.info(f"Raw api_host input: '{api_host}'")
        
        if ':' in api_host:
            host_part, port_part = api_host.split(':', 1)
            try:
                port = int(port_part)
            except ValueError:
                host_part = api_host
                port = 8006
        else:
            host_part = api_host
            port = 8006
        
        # Clean host part and ensure it's properly formatted
        host_part = host_part.strip()
        logger.info(f"Parsed host: '{host_part}', port: {port}")
        
        # Use properties to get user and token name from the full token_id
        user = cluster.api_user
        token_name = cluster.api_token_name
        
        logger.info(f"Connecting to Proxmox API at {host_part}:{port} with user: {user}, token: {token_name}")
        
        # Create the full URL to see what's being constructed
        full_url = f"https://{host_part}:{port}"
        logger.info(f"Full URL being constructed: {full_url}")
        
        proxmox = ProxmoxAPI(
            host=host_part,
            port=port,
            user=user,
            token_name=token_name,
            token_value=cluster.api_token_secret,
            verify_ssl=False,
            timeout=30
        )
        
        # Test the connection
        proxmox.nodes.get()
        return proxmox
        
    except requests.exceptions.SSLError as e:
        logger.error(f"SSL Error connecting to {api_host}: {str(e)}")
        raise Exception(f"SSL Error: Could not verify SSL certificate for {api_host}")
    except requests.exceptions.ConnectionError as e:
        error_msg = str(e)
        logger.error(f"Connection Error to {api_host}: {error_msg}")
        
        # Handle specific IPv6 URL error
        if "Invalid IPv6 URL" in error_msg:
            raise Exception(f"Invalid URL format. Please use format: IP:PORT (e.g., 192.168.1.100:8006) without protocol prefix")
        else:
            raise Exception(f"Connection Error: Could not connect to {api_host}")
    except requests.exceptions.Timeout as e:
        logger.error(f"Timeout connecting to {api_host}: {str(e)}")
        raise Exception(f"Timeout: Connection to {api_host} timed out")
    except ValueError as e:
        error_msg = str(e)
        logger.error(f"ValueError connecting to {api_host}: {error_msg}")
        
        # Handle IPv6 URL parsing errors
        if "Invalid IPv6 URL" in error_msg or "URL" in error_msg:
            raise Exception(f"Invalid URL format. Please enter just the IP address and port (e.g., 192.168.1.100:8006)")
        else:
            raise Exception(f"Invalid value: {error_msg}")
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error connecting to {api_host}: {error_msg}")
        
        # Handle any other IPv6 related errors
        if "Invalid IPv6 URL" in error_msg or "IPv6" in error_msg:
            raise Exception(f"URL parsing error. Please ensure you enter only the IP address and port (e.g., 192.168.1.100:8006)")
        else:
            raise Exception(f"Failed to connect: {error_msg}")

def get_cluster_overview(proxmox):
    """Get cluster overview statistics"""
    global_storages = set()
    try:
        total_cpu = 0
        used_cpu = 0
        total_memory = 0
        used_memory = 0
        total_storage = 0
        used_storage = 0
        
        # Get nodes statistics
        for node in proxmox.nodes.get():
            node_status = proxmox.nodes(node['node']).status.get()
            
            # CPU
            cpu_count = node_status.get('cpuinfo', {}).get('cpus', 0)
            cpu_usage = float(node_status.get('cpu', 0))
            logger.warning(f"Node {node['node']}: CPU count = {cpu_count}, CPU usage = {cpu_usage}")
            total_cpu += cpu_count
            used_cpu += cpu_usage
            
            # Memory
            total_memory += node_status.get('memory', {}).get('total', 0)
            used_memory += node_status.get('memory', {}).get('used', 0)
            
            # Storage
            for storage in proxmox.nodes(node['node']).storage.get():
                storage_name = storage.get('storage', '')
                storage_type = storage.get('type', '')
                storage_id = f"{storage_name}:{storage_type}"
                
                logger.warning(f"Processing storage: {storage_name} (type: {storage_type}) on node {node['node']}")
                
                try:
                    storage_status = proxmox.nodes(node['node']).storage(storage_name).status.get()
                    
                    storage_total = storage_status.get('total', 0)
                    storage_used = storage_status.get('used', 0)
                    
                    logger.warning(f"Storage {storage_name} stats - Total: {storage_total}, Used: {storage_used}")
                    
                    # Check if this is a shared storage or local storage
                    if storage_type in ['glusterfs', 'nfs', 'cifs', 'zfspool'] and storage_id in global_storages:
                        logger.warning(f"Skipping duplicate shared storage {storage_name}")
                        continue
                    
                    if storage_type in ['glusterfs', 'nfs', 'cifs', 'zfspool']:
                        logger.warning(f"Adding shared storage {storage_name} to total")
                        global_storages.add(storage_id)
                    else:
                        logger.warning(f"Adding local storage {storage_name} to total")
                    
                    total_storage += storage_total
                    used_storage += storage_used
                    
                except Exception as storage_error:
                    logger.warning(f"Could not get storage stats for {storage_name}: {storage_error}")
        
        # Calculate averages for CPU
        node_count = len([node for node in proxmox.nodes.get() if node.get('status') == 'online'])
        if node_count > 0:
            # used_cpu is already in percentage (0-1), so we just need to average it
            used_cpu_avg = used_cpu / node_count if node_count > 1 else used_cpu
            # Convert to percentage (0-100)
            cpu_usage_percent = used_cpu_avg * 100
        else:
            used_cpu_avg = 0
            cpu_usage_percent = 0
        
        logger.warning(f"Total CPU: {total_cpu}, Used CPU sum: {used_cpu}, Node count: {node_count}")
        
        return {
            'cpu': {
                'total': total_cpu,
                'used': used_cpu_avg,
                'usage_percent': cpu_usage_percent
            },
            'memory': {
                'total': total_memory,
                'used': used_memory,
                'usage_percent': (used_memory / total_memory * 100) if total_memory > 0 else 0
            },
            'storage': {
                'total': total_storage,
                'used': used_storage,
                'usage_percent': (used_storage / total_storage * 100) if total_storage > 0 else 0
            },
            'nodes': node_count
        }
    except Exception as e:
        logger.error(f"Error getting cluster overview: {str(e)}")
        raise
