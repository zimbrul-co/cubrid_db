"""
Database Creation Module for CUBRID Backend in Django

This module is part of the Django backend for CUBRID databases and is responsible for
handling the creation, modification, and deletion of database schemas. It includes
classes and methods that facilitate the management of database tables, indexes, and
other schema elements in alignment with the Django ORM models.

Key Components:
- DatabaseCreation: A class that is tailored to manage database creation and schema
  operations for the CUBRID database. It includes methods for creating tables,
  setting up indexes, and handling other database-specific schema tasks.
- Supporting functions: These functions provide additional utilities for managing
  database schemas, such as applying migrations or custom SQL scripts.

This module is typically used internally by Django during the process of migrating
database schemas, reflecting model changes in the database structure. It ensures
that the Django models are accurately and efficiently represented in the CUBRID
database schema.

Usage:
This module is not usually directly used by Django developers. Instead, it is
utilized by Django's migration and management commands.

Note:
- Understanding of Django's ORM and migration system, as well as familiarity with
  CUBRID's database schema capabilities, is essential for customizing or extending
  the functionalities of this module.
- This module is specific to the Django backend for CUBRID and may contain
  CUBRID-specific implementations and considerations.
"""
import sys
import time
import subprocess

from django.db.backends.base.creation import BaseDatabaseCreation


class DatabaseCreation(BaseDatabaseCreation):
    """
    Database creation class for CUBRID database in Django.

    This class extends Django's BaseDatabaseCreation and is responsible for managing
    the creation and destruction of the test database in a CUBRID environment. It
    provides CUBRID-specific implementations for setting up and tearing down the test
    database, ensuring compatibility with CUBRID's database management commands.

    Methods:
    _create_test_db: Creates the test database using CUBRID commands.
    _destroy_test_db: Destroys the test database, cleaning up any resources.
    """

    def _create_test_db(self, verbosity, autoclobber, keepdb=False):
        """
        Creates the test database for CUBRID.

        This internal method sets up the test database by executing CUBRID commands
        to create, start, and check the database. It handles the case where a test
        database may already exist and provides an option to recreate it if necessary.

        Parameters:
        verbosity (int): The verbosity level.
        autoclobber (bool): Whether to automatically overwrite the existing test database
                            without confirmation.
        keepdb (bool): If True, keeps the existing database if it already exists. Defaults
                    to False.

        Returns:
        str: The name of the test database that was created or found.

        If 'keepdb' is True and the test database already exists, this method checks the
        database and returns its name. If the database does not exist or 'keepdb' is False,
        it proceeds to create and start a new test database. If an error occurs during
        database creation and 'autoclobber' is False, it prompts the user to confirm
        deletion and recreation of the test database.
        """
        test_database_name = self._get_test_db_name()

        # Create the test database and start the cubrid server.
        check_command = ["cubrid", "checkdb", test_database_name]
        create_command = ["cubrid", "createdb" , "--db-volume-size=20M",
                          "--log-volume-size=20M", test_database_name, "en_US.utf8"]
        start_command = ["cubrid", "server", "start", test_database_name]
        stop_command = ["cubrid", "server", "stop", test_database_name]
        delete_command = ["cubrid", "deletedb", test_database_name]

        if keepdb:
            # Check if the test database already exists
            try:
                subprocess.run(check_command, check = True)
                print("Database already exists")
                return test_database_name
            except subprocess.CalledProcessError:
                pass

        try:
            cp = subprocess.run(create_command, capture_output = True, check = False)
            self.log(cp.stdout.decode())
            self.log(cp.stderr.decode())
            cp.check_returncode()
            print('Created')
            subprocess.run(start_command, check = True)
            print('Started')
            subprocess.run(check_command, check = True)
            self.connection.cursor()
            print('Checked')

        except subprocess.CalledProcessError as e:
            self.log(f"Error creating the test database: {e}")
            if not autoclobber:
                confirm = input(f"Type 'yes' if you would like to try deleting the test '\
                                'database '{test_database_name}', or 'no' to cancel: ")
            if autoclobber or confirm == 'yes':
                try:
                    if verbosity >= 1:
                        print("Destroying old test database...")
                        subprocess.run(stop_command, check = True)
                        subprocess.run(delete_command, check = True)

                        print("Creating test database...")
                        cp = subprocess.run(create_command, capture_output = True, check = False)
                        self.log(cp.stdout.decode())
                        self.log(cp.stderr.decode())
                        cp.check_returncode()
                        print('Created')

                        subprocess.run(start_command, check = True)
                        print('Started')
                except subprocess.CalledProcessError as e2:
                    self.log(f"Error recreating the test database: {e2}")
                    sys.exit(2)
            else:
                print( "Tests cancelled.")
                sys.exit(1)

        return test_database_name

    def _destroy_test_db(self, test_database_name, verbosity):
        """
        Removes the test database for CUBRID.

        This internal method is responsible for destroying the test database created
        during testing. It stops the CUBRID server and deletes the test database using
        CUBRID commands.

        Parameters:
        test_database_name (str): The name of the test database to be destroyed.
        verbosity (int): The verbosity level.

        The method attempts to stop the server and delete the test database. If an error
        occurs during this process, it logs the error. Finally, it closes the database
        connection.
        """
        time.sleep(1) # To avoid "database is being accessed by other users" errors.
        try:
            subprocess.run(["cubrid", "server", "stop", test_database_name], check = True)
            subprocess.run(["cubrid", "deletedb", test_database_name], check = True)
        except subprocess.CalledProcessError as e:
            self.log(f"Error destroying the test database: {e}")
        finally:
            self.connection.close()
