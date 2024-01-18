import sys
import time
import subprocess

from django.db.backends.base.creation import BaseDatabaseCreation

# The prefix to put on the default database name when creating
# the test database.
TEST_DATABASE_PREFIX = 'test_'


class DatabaseCreation(BaseDatabaseCreation):
    def _create_test_db(self, verbosity, autoclobber, keepdb=False):
        """Internal implementation - creates the test db tables."""
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
        "Internal implementation - remove the test db tables."
        # Remove the test database to clean up after
        # ourselves. Connect to the previous database (not the test database)
        # to do so, because it's not allowed to delete a database while being
        # connected to it.
        time.sleep(1) # To avoid "database is being accessed by other users" errors.
        try:
            subprocess.run(["cubrid", "server", "stop", test_database_name], check = True)
            subprocess.run(["cubrid", "deletedb", test_database_name], check = True)
        except subprocess.CalledProcessError as e:
            self.log(f"Error destroying the test database: {e}")
        finally:
            self.connection.close()
