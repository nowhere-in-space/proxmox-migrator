from flask import Flask, render_template, request, jsonify, flash, redirect, url_for, session, send_from_directory
import logging
import json
from datetime import datetime
from urllib3.exceptions import InsecureRequestWarning
import urllib3

# Suppress insecure HTTPS warnings
urllib3.disable_warnings(InsecureRequestWarning)

# Import our modules
from config import Config
from models import db, Cluster, AdminUser
from auth import login_required, init_admin_user
from proxmox_client import connect_to_proxmox, get_cluster_overview
from migration_service import migrate_vm
from disk_service import get_migration_status
from utils import format_size, get_vm_info
from database_migrations import run_database_migrations

# Initialize Flask app
app = Flask(__name__)
app.config.from_object(Config)

# Initialize database
db.init_app(app)

# Configure logging
import os
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s %(levelname)s:%(name)s:%(message)s',
    handlers=[
        logging.FileHandler('logs/proxmox_migrator.log'),
        logging.StreamHandler()
    ]
)

# Initialize database tables if needed
def init_db():
    try:
        with app.app_context():
            # Run database migrations first
            db_path = os.path.join(app.instance_path, 'proxmox_clusters.db')
            if not run_database_migrations(db_path):
                app.logger.error("Database migrations failed")
                return False
            
            # Create all tables
            db.create_all()
            app.logger.info("Database initialized successfully")
            return True
    except Exception as e:
        app.logger.error(f"Database initialization error: {e}")
        return False

# Initialize database
init_db()

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(app.template_folder, 'favicon.ico', mimetype='image/vnd.microsoft.icon')

@app.route('/')
@login_required
def index():
    clusters = Cluster.query.all()
    # Fast loading - returning only basic information about clusters
    clusters_overview = []
    
    for cluster in clusters:
        clusters_overview.append({
            'cluster': cluster,
            'overview': None,  # Statistics will be loaded asynchronously
            'status': 'loading',
            'error': None
        })
    
    return render_template('index.html', clusters_overview=clusters_overview)

@app.route('/cluster/<int:cluster_id>/overview')
@login_required
def get_cluster_overview_ajax(cluster_id):
    """AJAX endpoint for getting cluster statistics"""
    cluster = Cluster.query.get_or_404(cluster_id)
    
    try:
        proxmox = connect_to_proxmox(cluster)
        overview = get_cluster_overview(proxmox)
        
        # Format sizes for display
        overview['memory']['total_formatted'] = format_size(overview['memory']['total'])
        overview['memory']['used_formatted'] = format_size(overview['memory']['used'])
        overview['storage']['total_formatted'] = format_size(overview['storage']['total'])
        overview['storage']['used_formatted'] = format_size(overview['storage']['used'])
        
        return jsonify({
            'status': 'online',
            'overview': overview,
            'error': None
        })
    except Exception as e:
        return jsonify({
            'status': 'offline',
            'overview': None,
            'error': str(e)
        }), 500

@app.route('/login', methods=['GET', 'POST'])
def login():
    admin = init_admin_user()
        
    if request.method == 'POST':
        password = request.form.get('password')
        
        if admin.is_first_login:
            # First login - set password
            admin.set_password(password)
            db.session.commit()
            session['logged_in'] = True
            session.permanent = True
            flash('Password set successfully!', 'success')
            return redirect(url_for('index'))
        else:
            # Normal login
            if admin.check_password(password):
                session['logged_in'] = True
                session.permanent = True
                flash('Logged in successfully!', 'success')
                return redirect(url_for('index'))
            else:
                flash('Invalid password', 'danger')
                
    return render_template('login.html', first_login=admin.is_first_login)

@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    flash('Logged out successfully!', 'info')
    return redirect(url_for('login'))

@app.route('/change_password', methods=['GET', 'POST'])
@login_required
def change_password():
    if request.method == 'POST':
        current_password = request.form.get('current_password')
        new_password = request.form.get('new_password')
        confirm_password = request.form.get('confirm_password')
        
        admin = AdminUser.query.first()
        
        if not admin.check_password(current_password):
            flash('Current password is incorrect', 'danger')
        elif new_password != confirm_password:
            flash('New passwords do not match', 'danger')
        else:
            admin.set_password(new_password)
            db.session.commit()
            flash('Password changed successfully!', 'success')
            return redirect(url_for('index'))
    
    return render_template('change_password.html')

@app.route('/add_cluster', methods=['GET', 'POST'])
@login_required
def add_cluster():
    if request.method == 'POST':
        ssh_port = request.form.get('ssh_port', '22')
        try:
            ssh_port = int(ssh_port)
        except ValueError:
            ssh_port = 22
            
        cluster = Cluster(
            name=request.form['name'],
            api_host=request.form['api_host'],
            api_token_id=request.form['api_token_id'],
            api_token_secret=request.form['api_token_secret'],
            ssh_password=request.form['ssh_password'],
            ssh_port=ssh_port
        )
        
        try:
            # Test connection
            proxmox = connect_to_proxmox(cluster)
            
            db.session.add(cluster)
            db.session.commit()
            flash('Cluster added successfully!', 'success')
            return redirect(url_for('index'))
        except Exception as e:
            flash(f'Error connecting to cluster: {str(e)}', 'danger')
    
    return render_template('add_cluster.html')

@app.route('/cluster/<int:cluster_id>/delete', methods=['POST'])
@login_required
def delete_cluster(cluster_id):
    """Delete a cluster"""
    try:
        cluster = Cluster.query.get_or_404(cluster_id)
        cluster_name = cluster.name
        
        # Delete the cluster from database
        db.session.delete(cluster)
        db.session.commit()
        
        flash(f'Cluster "{cluster_name}" has been deleted successfully!', 'success')
        return jsonify({'success': True, 'message': f'Cluster "{cluster_name}" deleted successfully'})
        
    except Exception as e:
        db.session.rollback()
        error_msg = f'Error deleting cluster: {str(e)}'
        flash(error_msg, 'danger')
        return jsonify({'success': False, 'error': error_msg}), 500

@app.route('/cluster/<int:cluster_id>/rename', methods=['POST'])
@login_required
def rename_cluster(cluster_id):
    """Rename a cluster"""
    try:
        cluster = Cluster.query.get_or_404(cluster_id)
        old_name = cluster.name
        new_name = request.form.get('name', '').strip()
        
        if not new_name:
            return jsonify({'success': False, 'error': 'New name is required'}), 400
        
        # Check if name already exists
        existing_cluster = Cluster.query.filter_by(name=new_name).first()
        if existing_cluster and existing_cluster.id != cluster_id:
            return jsonify({'success': False, 'error': f'Cluster with name "{new_name}" already exists'}), 400
        
        # Update the cluster name
        cluster.name = new_name
        db.session.commit()
        
        flash(f'Cluster renamed from "{old_name}" to "{new_name}" successfully!', 'success')
        return jsonify({'success': True, 'message': f'Cluster renamed to "{new_name}" successfully'})
        
    except Exception as e:
        db.session.rollback()
        error_msg = f'Error renaming cluster: {str(e)}'
        flash(error_msg, 'danger')
        return jsonify({'success': False, 'error': error_msg}), 500

@app.route('/cluster/<int:cluster_id>/vms')
@login_required
def list_vms(cluster_id):
    cluster = Cluster.query.get_or_404(cluster_id)
    
    try:
        proxmox = connect_to_proxmox(cluster)
        
        # Get all VMs from all nodes
        vms = []
        for node in proxmox.nodes.get():
            try:
                node_vms = proxmox.nodes(node['node']).qemu.get()
                for vm in node_vms:
                    vm_info = get_vm_info(proxmox, node['node'], vm['vmid'])
                    vms.append(vm_info)
            except Exception as e:
                app.logger.warning(f"Could not get VMs from node {node['node']}: {e}")
        
        # Sort VMs
        sort_by = request.args.get('sort', 'vmid')
        direction = request.args.get('dir', 'asc')
        
        reverse = direction == 'desc'
        if sort_by in ['vmid', 'memory', 'cores']:
            vms.sort(key=lambda x: int(x[sort_by]), reverse=reverse)
        else:
            vms.sort(key=lambda x: str(x[sort_by]), reverse=reverse)
        
        # Get destination clusters
        dest_clusters = Cluster.query.filter(Cluster.id != cluster_id).all()
        
        return render_template('list_vms.html', 
                             cluster=cluster, 
                             vms=vms, 
                             dest_clusters=dest_clusters,
                             current_sort=sort_by,
                             current_dir=direction)
    except Exception as e:
        flash(f'Error connecting to cluster: {str(e)}', 'danger')
        return redirect(url_for('index'))

@app.route('/cluster/<int:cluster_id>/resources')
@login_required
def get_cluster_resources(cluster_id):
    cluster = Cluster.query.get_or_404(cluster_id)
    
    try:
        proxmox = connect_to_proxmox(cluster)
        
        # Get nodes
        nodes = []
        for node in proxmox.nodes.get():
            node_status = proxmox.nodes(node['node']).status.get()
            nodes.append({
                'name': node['node'],
                'status': node['status'],
                'cpu': float(node_status.get('cpu', 0)),
                'memory': node_status.get('memory', {}).get('used', 0)
            })
        
        # Get storage pools
        storage_pools = []
        processed_storage = set()
        
        for node in proxmox.nodes.get():
            for storage in proxmox.nodes(node['node']).storage.get():
                storage_name = storage.get('storage', '')
                storage_type = storage.get('type', '')
                
                # Avoid duplicates for shared storage
                if storage_type in ['glusterfs', 'nfs', 'cifs'] and storage_name in processed_storage:
                    continue
                
                storage_pools.append({
                    'name': storage_name,
                    'type': storage_type,
                    'shared': storage_type in ['glusterfs', 'nfs', 'cifs']
                })
                processed_storage.add(storage_name)
        
        # Get network bridges
        network_bridges = []
        processed_bridges = set()
        
        for node in proxmox.nodes.get():
            try:
                # Get network configuration for the node
                network_config = proxmox.nodes(node['node']).network.get()
                for net_interface in network_config:
                    if net_interface.get('type') == 'bridge':
                        bridge_name = net_interface.get('iface', '')
                        if bridge_name and bridge_name not in processed_bridges:
                            network_bridges.append({
                                'name': bridge_name,
                                'type': 'bridge',
                                'node': node['node']
                            })
                            processed_bridges.add(bridge_name)
            except Exception as e:
                app.logger.warning(f"Could not get network config from node {node['node']}: {e}")
        
        return jsonify({
            'nodes': nodes,
            'storage_pools': storage_pools,
            'network_bridges': network_bridges
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/cluster/<int:cluster_id>/vm/<vmid>/config')
@login_required
def get_vm_config(cluster_id, vmid):
    """Get VM configuration for network mapping"""
    try:
        cluster = Cluster.query.get_or_404(cluster_id)
        proxmox = connect_to_proxmox(cluster)
        node = request.args.get('node')
        
        if not node:
            return jsonify({'error': 'Node parameter is required'}), 400
            
        vm_config = proxmox.nodes(node).qemu(vmid).config.get()
        return jsonify(vm_config)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/migration-status')
@login_required
def get_migration_status_endpoint():
    """Endpoint to get current migration status"""
    return jsonify(get_migration_status())

@app.route('/confirm-vm-stop', methods=['POST'])
@login_required
def confirm_vm_stop():
    """Endpoint to confirm VM stop during migration"""
    try:
        from disk_service import migration_status
        migration_status['stop_confirmed'] = True
        app.logger.warning("Received confirmation to stop VM during migration")
        return jsonify({'status': 'success', 'message': 'VM stop confirmed'})
    except Exception as e:
        error_msg = f"Error confirming VM stop: {str(e)}"
        app.logger.error(error_msg)
        return jsonify({'status': 'error', 'message': error_msg}), 500

@app.route('/cancel-migration', methods=['POST'])
@login_required
def cancel_migration():
    """Endpoint to cancel an in-progress migration"""
    try:
        from disk_service import migration_status
        migration_status['active'] = False
        app.logger.warning("Migration cancelled by user")
        return jsonify({'status': 'success', 'message': 'Migration cancelled'})
    except Exception as e:
        error_msg = f"Error cancelling migration: {str(e)}"
        app.logger.error(error_msg)
        return jsonify({'status': 'error', 'message': error_msg}), 500

@app.route('/migrate', methods=['POST'])
@login_required
def migrate_vm_endpoint():
    """Endpoint for VM migration"""
    try:
        data = request.json
        app.logger.warning(f"Received migration request: {json.dumps(data)}")
        
        # Start migration in background thread with app context
        import threading
        
        def migrate_with_context():
            with app.app_context():
                migrate_vm(data)
        
        thread = threading.Thread(target=migrate_with_context)
        thread.daemon = True
        thread.start()
        
        return jsonify({'status': 'success', 'message': 'Migration started'})
        
    except Exception as e:
        error_msg = f"Error starting migration: {str(e)}"
        app.logger.error(error_msg)
        return jsonify({'status': 'error', 'message': error_msg}), 500

@app.route('/health')
def health_check():
    """Health check endpoint for Docker"""
    try:
        # Simple database check
        AdminUser.query.first()
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'version': '1.0.0'
        }), 200
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
