"""
Django-CUBRID Backend

This module provides a Django database backend for CUBRID, a comprehensive open-source
relational database management system (RDBMS) highly optimized for web applications.
The Django-CUBRID backend facilitates seamless integration and management of CUBRID
databases within Django projects.

Features:
- Full support for Django's ORM capabilities: This backend leverages Django's ORM
  system, allowing developers to work with CUBRID databases using Django's standard
  models and query syntax.
- Transaction management: The backend provides support for Django's transaction
  handling features, ensuring data integrity and consistency.
- Efficient handling of database connections: It manages database connections
  efficiently, providing both persistent and non-persistent connection modes to suit
  different deployment scenarios.
- Compatibility with CUBRID features: It supports various CUBRID-specific features
  and data types, enabling the full power of the CUBRID database system within a
  Django application.

Requirements:
- Django (version as per compatibility): Ensure you have the compatible version of
  Django installed.
- CUBRID Database Server: This backend requires a running instance of the CUBRID
  database server.

Installation and Configuration:
- Install this backend by adding it to your Django project.
- Configure your Django settings to use the CUBRID database by setting the
  'ENGINE' field in the DATABASES setting to 'django_cubrid'.
- Provide the necessary database connection parameters in your settings file.

Example DATABASES configuration in settings.py:

DATABASES = {
    'default': {
        'ENGINE': 'django_cubrid',
        'NAME': 'your_database_name',
        'USER': 'your_username',
        'PASSWORD': 'your_password',
        'HOST': 'database_host',  # Set to empty string for localhost.
        'PORT': 'database_port',  # Set to empty string for default.
    }
}

For more information on using and configuring this backend, refer to the Django
documentation and the CUBRID database documentation.

Note:
- This backend is community-driven and is not officially part of the Django project.
- Always ensure compatibility between your Django version and this backend.
"""


VERSION = "0.2.0"
