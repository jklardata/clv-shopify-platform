import os
import sys
import subprocess
import certifi

def install_certificates():
    """Install certificates for Python on macOS."""
    try:
        # Get the Python.app path
        python_app = '/Applications/Python 3.11'
        if not os.path.exists(python_app):
            print(f"Python.app not found at {python_app}")
            return False

        # Install certificates using pip
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', '--upgrade', 'certifi'])
        
        # Get certifi's certificate path
        certifi_path = certifi.where()
        print(f"Certifi certificate path: {certifi_path}")
        
        # Set environment variable
        os.environ['SSL_CERT_FILE'] = certifi_path
        os.environ['REQUESTS_CA_BUNDLE'] = certifi_path
        
        # Create symlink to system certificates
        system_cert_path = '/private/etc/ssl/cert.pem'
        if os.path.exists(system_cert_path):
            print(f"System certificates found at {system_cert_path}")
            os.environ['SSL_CERT_FILE'] = system_cert_path
            os.environ['REQUESTS_CA_BUNDLE'] = system_cert_path
        
        print("Certificate paths configured successfully")
        return True
        
    except Exception as e:
        print(f"Error installing certificates: {str(e)}")
        return False

if __name__ == "__main__":
    if install_certificates():
        print("Certificates installed successfully")
    else:
        print("Failed to install certificates") 