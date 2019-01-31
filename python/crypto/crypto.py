import sys
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import serialization, hashes

def generate_key():
    """ Returns a cryptographic asymmetric key
    """
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )
    return private_key

def get_public_key(private_key):
    """ Returns the public key bound to the given private key
    """
    return private_key.public_key()

def generate_key_pair():
    priv, pub = generate_key()
    return private_key, private_key.public_key()

def serialize_private_key(private_key):
    """ If we ever wanted to export it to file/network.
        We probably don't. Returns a byte array.
    """
    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    return pem

def serialize_public_key(public_key):
    """ Serialize for export. Returns a byte array.
    """
    pem_pub = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )
    return pem_pub

def deserialize_public_key(data):
    """ Returns a Public Key object given serialized data (byte array)
    """
    return serialization.load_pem_public_key(data, backend=default_backend())

def encrypt_message(message, public_key):
    """ Encrypts a message (a UTF-8 string) for a given recipient, given the recipient's public key.
        Returns a byte array
    """
    message_bytes = message.encode('utf-8')
    ciphertext = public_key.encrypt(
        message_bytes,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )
    return ciphertext

def decrypt_message(ciphertext, private_key):
    """ Decrypts a message sent to us. They used our public key to enrypt.
        We use our private key to decrypt.
        Returns a UTF-8 string.
    """
    plaintext = private_key.decrypt(
        ciphertext,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )
    return plaintext.decode('utf-8')
