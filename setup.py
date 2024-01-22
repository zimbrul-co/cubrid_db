"""Setup script for the cubrid_db Python package."""

import os
import platform
import subprocess
import sys

from setuptools import setup, Extension


def get_script_dir():
    """Return the directory of the script."""
    path = os.path.abspath(sys.argv[0])
    if os.path.isdir(path):
        return path
    return os.path.dirname(path)


def get_platform():
    """Return the OS type and architecture"""
    arch = platform.architecture()[0]
    if arch == '32bit':
        arch = 'x86'
    elif arch == '64bit':
        arch = 'x64'
    else:
        raise OSError(f'The machine type cannot be determined from "{arch}".')

    os_type = platform.system()
    if os_type not in ['Windows', 'Linux']:
        raise OSError(f'Unsupported OS type: {os_type}')

    if os_type == 'Linux' and arch == 'x86':
        raise OSError('Unsupported platform: 32-bit Linux')

    return os_type, arch


OS_TYPE, ARCH_TYPE = get_platform()


cwd = os.getcwd()
script_dir = get_script_dir()
cci_dir = os.path.join(script_dir, "cci-src")
print ('script directory:', script_dir)
print ('CCI directory:', cci_dir)


if OS_TYPE == 'Windows':
    VCOMTOOLS_ENV = 'VS140COMNTOOLS'

    # Check for Visual Studio common tools
    if VCOMTOOLS_ENV in os.environ:
        print(f"'{VCOMTOOLS_ENV}' is set: {os.environ[VCOMTOOLS_ENV]}")
    else:
        raise EnvironmentError(f"Environment variable '{VCOMTOOLS_ENV}' is not set")

    # Build CCI
    os.chdir(os.path.join(cci_dir, "win\\cas_cci"))

    build_cci_bat = 'call "%VS140COMNTOOLS%vsvars32.bat\r\n"'\
                f'devenv cas_cci_v140_lib.vcxproj /build "release|{ARCH_TYPE}"\r\n'
    print('Generating CCI build script: build_cci.bat')
    print(build_cci_bat)
    with open("build_cci.bat", "w", encoding="utf8") as f_bat:
        f_bat.write(build_cci_bat)

    try:
        result = subprocess.run(["build_cci.bat"], check=True)
        print("CCI build script executed successfully")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Error in CCI build script execution: {e}") from e
    finally:
        os.chdir(cwd)

    inc_dir_base = os.path.join(cci_dir, "src\\base")
    inc_dir_cci = os.path.join(cci_dir, "src\\cci")

    if ARCH_TYPE == 'x86':
        lib_dir = os.path.join(cci_dir, "win\\cas_cci\\Win32\\Release")
        lib_dir_ex = os.path.join(cci_dir, "win\\external\\lib")
    else:
        lib_dir = os.path.join(cci_dir, "win\\cas_cci\\x64\\Release")
        lib_dir_ex = os.path.join(cci_dir, "win\\external\\lib64")

    cci_static_lib = os.path.join(lib_dir, 'cas_cci.lib')
    print('CCI static lib path:', cci_static_lib)

    # Use the CCI static library
    if os.path.isfile(cci_static_lib):
        ext_modules = [
            Extension(
                name="_cubrid",
                extra_link_args=["/NODEFAULTLIB:libcmt"],
                library_dirs=[lib_dir, lib_dir_ex],
                libraries=["cas_cci", "libregex38a",
                           "ws2_32", "oleaut32", "advapi32"],
                include_dirs=[inc_dir_base, inc_dir_cci],
                sources=['python_cubrid.c'],
            )
        ]
    else:
        raise FileNotFoundError(f"CCI static lib not found at {cci_static_lib}")

else:
    # Build CCI
    os.chdir(cci_dir)
    try:
        result = subprocess.run(['bash', "build.sh"], check=True)
        print("CCI build script executed successfully")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Error in CCI build script execution: {e}") from e
    finally:
        os.chdir(cwd)

    inc_dir_base = os.path.join(cci_dir, "src/base")
    inc_dir_cci = os.path.join(cci_dir, "src/cci")
    cci_static_lib = os.path.join(cci_dir, 'build_x86_64_release/cci/libcascci.a')
    openssl_lib = os.path.join(cci_dir, 'external/openssl/lib')
    ssl_static_lib = os.path.join(openssl_lib, 'libssl.a')
    crypto_static_lib = os.path.join(openssl_lib, 'libcrypto.a')
    print('CCI static lib path:', cci_static_lib)
    print('SSL static lib path:', ssl_static_lib)
    print('Crypto static lib path:', crypto_static_lib)

    # Use the CCI static library
    if os.path.isfile(cci_static_lib):  # use the CCI static library
        ext_modules = [
            Extension(
                name="_cubrid",
                include_dirs=[inc_dir_base, inc_dir_cci],
                sources=['python_cubrid.c'],
                libraries=["pthread", "stdc++"],
                extra_objects=[
                    cci_static_lib,
                    ssl_static_lib,
                    crypto_static_lib,
                ]
            )
        ]
    else:
        raise FileNotFoundError(f"CCI static lib not found at {cci_static_lib}")


# Read the version file
with open('VERSION', 'r', encoding='utf-8') as version_file:
    version = version_file.read().strip()


# set py_modules
py_modules = [
    "cubrid_db.connections",
    "cubrid_db.cursors",
    "cubrid_db.field_type",
    "django_cubrid.base",
    "django_cubrid.client",
    "django_cubrid.compiler",
    "django_cubrid.creation",
    "django_cubrid.features",
    "django_cubrid.introspection",
    "django_cubrid.operations",
    "django_cubrid.schema",
    "django_cubrid.validation",
]


# Install CUBRID-Python driver.
setup(
    name="CUBRID-Python",
    version=version,
    description="Python interface to CUBRID",
    long_description=\
            "Python interface to CUBRID conforming to the python DB API 2.0 "
            "specification.\n"
            "See http://www.python.org/topics/database/DatabaseAPI-2.0.html.",
    py_modules=py_modules,
    author="Casian Andrei",
    author_email="casian@zco.ro",
    license="BSD",
    url="https://github.com/zimbrul-co/cubrid-python",
    ext_modules=ext_modules
)
