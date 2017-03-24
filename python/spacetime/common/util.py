'''
Created on Jul 7, 2016

@author: Arthur Valadares
'''
import platform

def get_os():
    if platform.system() == 'Windows':
        return 'Windows'
    elif platform.system().startswith("CYGWIN"):
        return 'Windows CYGWIN'
    elif platform.system() == 'Java':
        import java.lang.System
        return java.lang.System.getProperty('os.name')
    else:
        return platform.system()