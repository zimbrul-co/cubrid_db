from django.db.backends.base.client import BaseDatabaseClient


class DatabaseClient(BaseDatabaseClient):
    executable_name = 'csql'

    @classmethod
    def settings_to_cmd_args_env(cls, settings_dict, parameters):
        args = [cls.executable_name]

        database = settings_dict["OPTIONS"].get(
            "database",
            settings_dict["OPTIONS"].get("db", settings_dict["NAME"]),
        )
        user = settings_dict["OPTIONS"].get("user", settings_dict["USER"])
        password = settings_dict["OPTIONS"].get(
            "password",
            settings_dict["OPTIONS"].get("passwd", settings_dict["PASSWORD"]),
        )
        host = settings_dict["OPTIONS"].get("host", settings_dict["HOST"])

        if user:
            args += ["-u", user]
        if password:
            args += ["-p", password]
        if database:
            if host:
                args += [f'{database}@{host}']
            else:
                args += [database]

        args.extend(parameters)
        return args, None
