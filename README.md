# Proxmox VM Migration Tool

Web application for migrating virtual machines between Proxmox VE clusters.

## Description

This tool allows you to migrate virtual machines between different Proxmox clusters through a web interface. Supports migration of both file-based storage (directory, NFS) and block-based storage (LVM, ZFS).

## Features

- 🔐 Authentication system with administrator password
- 🖥️ Web interface for cluster and migration management
- 📊 Display VM lists with their characteristics
- 🔄 Migration between different storage types
- 📋 Real-time migration progress tracking
- 🗂️ Automatic disk renaming for new VM ID
- 🌐 Network interface configuration for target cluster

## System Requirements

- Python 3.8+
- SSH access to Proxmox VE clusters
- Web browser for interface access

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd panteon-datacenter
```

2. Create a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# or
.venv\Scripts\activate     # Windows
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure environment variables (create `.env` file):
```env
SECRET_KEY=your-secret-key-here
ADMIN_PASSWORD=your-admin-password
```

5. Run database migration (if upgrading from previous version):
```bash
python migrate_db.py
```

## Running

### Local Development
```bash
python app.py
```

The application will be available at: `http://localhost:5000`

### Using Docker

#### Build and run with Docker:
```bash
# Build the Docker image
docker build -t proxmox-migrator .

# Run the container
docker run -d \
  --name proxmox-migrator \
  -p 5000:5000 \
  -e SECRET_KEY="your-secret-key-here" \
  -e ADMIN_PASSWORD="your-admin-password" \
  -v $(pwd)/instance:/app/instance \
  -v $(pwd)/logs:/app/logs \
  -v $(pwd)/temp_migration:/app/temp_migration \
  proxmox-migrator
```

#### Using Docker Compose (Recommended):
```bash
# Create environment file
cp .env.example .env
# Edit .env with your settings

# Start the application
docker-compose up -d

# View logs
docker-compose logs -f

# Stop the application
docker-compose down
```

### Environment Variables

Required environment variables for Docker deployment:
- `SECRET_KEY` - Flask secret key for session management
- `ADMIN_PASSWORD` - Administrator password for web interface
- `FLASK_ENV` - Set to `production` for production deployment

### Docker Volumes

The application uses the following directories that should be mounted as volumes:
- `/app/instance` - Database files (SQLite)
- `/app/logs` - Application logs
- `/app/temp_migration` - Temporary files during migration

### Ports

- `5000` - Web interface (HTTP)

### Health Check

The Docker container includes a health check endpoint at `/health` that verifies:
- Database connectivity
- Application status
- Service timestamp

## Usage

1. **Authentication**: Enter the administrator password to access the system

2. **Adding clusters**: 
   - Go to "Add cluster" section
   - Specify connection data for Proxmox API and SSH

3. **View VMs**:
   - Select a cluster to view the list of virtual machines
   - Review VM characteristics before migration

4. **Migration**:
   - Select source and target VMs
   - Configure migration parameters (network, storage)
   - Start the process and track progress

## Project Structure

```
├── app.py                 # Main Flask application
├── auth.py               # Authentication module
├── config.py             # Application configuration
├── models.py             # Database models
├── proxmox_client.py     # Proxmox API client
├── migration_service.py  # VM migration service
├── disk_service.py       # Disk operations service
├── utils.py              # Utility functions
├── templates/            # HTML templates
├── instance/             # Database files
├── logs/                 # Application logs
└── temp_migration/       # Temporary migration files
```

## Supported Storage Types

### File-based Storage
- `dir` - Directory
- `nfs` - Network File System
- `cifs` - Common Internet File System

### Block-based Storage  
- `lvm` - Logical Volume Manager
- `lvmthin` - LVM Thin Provisioning
- `zfs` - ZFS
- `rbd` - Ceph RBD

## Migration Features

- **Automatic renaming**: Disks are automatically renamed to match the new VM ID
- **Cross-platform**: Correct path handling for different operating systems
- **Security**: All file transfers occur through secure SSH connections
- **Monitoring**: Detailed logging of all operations

## Logging

All operations are recorded in the `logs/proxmox_migrator.log` file. Logs include:
- Cluster connection information
- Migration process details
- Errors and warnings
- File transfer statistics

## Security

- Authentication via administrator password
- Storage of sensitive data in environment variables
- Secure SSH connections for file transfers
- Input data validation

## Development

For development, it is recommended to:

1. Use a virtual environment
2. Enable debug mode in `config.py`
3. Regularly check application logs
4. Test on test clusters

## License

This project is developed for internal use by Panteon company.

## Support

If you have questions or issues, contact the development team.
