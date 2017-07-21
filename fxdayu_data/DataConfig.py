DEFAULT = "main"


def get(name=DEFAULT):
    paths = get_config_paths()

    if name in paths:
        return paths[name]
    else:
        if name == DEFAULT:
            raise KeyError("Main config path not set, "
                           "use command 'DataAPI config set path' to set main config path")
        else:
            raise KeyError("%s not in paths, "
                           "use command 'DataAPI config add --name name --path path' to add config path" % name)


def config_paths_file():
    import os

    root = os.environ.get("fxdayu", os.path.expanduser(os.path.join('~', '.fxdayu')))
    if not os.path.exists(root):
        os.makedirs(root)

    return os.path.join(root, 'DataAPIConfig.json')


def get_config_paths():
    import json

    try:
        return json.load(open(config_paths_file()))
    except IOError:
        return {}


def set_config_paths(paths):
    import json

    file_path = config_paths_file()
    json.dump(paths, open(file_path, 'w'))
