import os
import ssl
import certifi

CERT_PATH = "/private/etc/ssl/cert.pem"

def install_certificates():
    """Install certificates for Python on macOS."""
    if not os.path.exists(CERT_PATH):
        print(f"Certificate file not found at {CERT_PATH}")
        return False
        
    try:
        ssl_context = ssl.create_default_context()
        ssl_context.load_verify_locations(CERT_PATH)
        print("Successfully loaded system certificates")
        return True
    except Exception as e:
        print(f"Error loading certificates: {str(e)}")
        return False

if __name__ == "__main__":
    success = install_certificates()
    if success:
        print("Certificates installed successfully")
    else:
        print("Failed to install certificates") 