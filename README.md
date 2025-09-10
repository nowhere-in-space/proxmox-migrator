# Proxmox VM Migration Tool [non-production use only]

Web application for migrating virtual machines between Proxmox VE clusters.

![Dashboard](images/dashboard.png)

## Description

This tool allows you to migrate virtual machines between different Proxmox clusters through a web interface. Supports migration of both file-based storage (directory, NFS) and block-based storage (LVM, ZFS).

## Features

- 🔐 Authentication system with administrator password
- 🖥️ Web interface for cluster and migration management
- 📊 Display VM lists with their characteristics
![VM list](images/vm_list.png)
- 🔄 Migration between different storage types
![VM list](images/migration_select.png)
- 📋 Real-time migration progress tracking
![VM list](images/migration_process.png)
- 🌐 Network interface configuration for target cluster

## System Requirements

- Python 3.8+
- SSH access to Proxmox VE clusters
- Web browser for interface access
- Enough free space for vm disks cache
- Tested on Proxmox VE 8.4.9. Based on API reference, all 8.X versions are supported. 

## Installation

1. Clone the repository:
```bash
git clone https://github.com/nowhere-in-space/proxmox-migrator
cd proxmox-migrator
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
├── app.py                    # Main Flask application with auto-migrations
├── auth.py                   # Authentication module
├── config.py                 # Application configuration
├── models.py                 # Database models (SQLAlchemy)
├── database_migrations.py    # Automatic database schema migrations
├── proxmox_client.py         # Proxmox API client
├── migration_service.py      # VM migration service
├── disk_service.py           # Disk operations service
├── utils.py                  # Utility functions
├── templates/                # HTML templates (Jinja2)
├── instance/                 # Database files (SQLite)
├── logs/                     # Application logs
└── temp_migration/           # Temporary migration files
```

### 🔄 **Automatic Database Migrations**

The application includes an automatic database migration system that:
- **Runs on startup**: Migrations are applied automatically when the application starts
- **Version tracking**: Uses `migration_version` table to track applied migrations
- **Safe upgrades**: Ensures schema changes are applied incrementally
- **No manual intervention**: No need to run separate migration scripts

## Supported Storage Types

### File-based Storage
- `dir` - Directory
- `nfs` - Network File System
- `cifs` - Common Internet File System

### Block-based Storage (partially tested)
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

This project use MIT License.
