import collections
import logging
import os
import yaml

logger = logging.getLogger(__name__)


class ConfigurationError(Exception):
    pass


class ConfigItem(object):
    def __init__(self, required=False, default=None, help='', multiple=False):
        self.help = help
        self.required = required
        self.multiple = multiple
        self.default = self.convert(default)

    def convert(self, item, manager=None):
        if not self.multiple:
            return item
        elif (isinstance(item, collections.Sequence)
              and not isinstance(item, basestring)):
            return item
        else:
            return [item]


class ConfigSection(collections.Mapping):
    def __init__(self, required=True, help='', config_description=None):
        self.config_description = config_description
        self.help = help
        self.required = required
        self.default = None

    def convert(self, item, manager):
        return manager.wrap(item, self.config_description)

    def __len__(self):
        return len(self.config_description)

    def __iter__(self):
        return iter(self.config_description)

    def __getitem__(self, key):
        return self.config_description[key]


class ConfigManager(collections.Mapping):

    @classmethod
    def wrap(cls, conf, config_description):
        if hasattr(conf, 'check_config'):
            wrapped_conf = conf
        else:
            wrapped_conf = cls(conf, config_description)
        return wrapped_conf

    def __init__(self, config_dict, config_description):
        self.config_paths = []
        self._configs = dict()
        self._description = config_description
        self._required = set()
        self._defaults = dict()
        for k, item in self._description.items():
            if item.required:
                self._required.add(k)
            if item.default is not None:
                self._defaults[k] = item.default
        for k, item in config_dict.items():
            if k in self._description:
                self._configs[k] = self._description[k].convert(item, self)
            else:
                self._configs[k] = item
        self._keys = set(self._defaults.keys() + self._configs.keys())

    def __len__(self):
        return len(self._keys)

    def __iter__(self):
        return iter(self._keys)

    def __getitem__(self, key):
        if key in self._configs:
            return self._configs[key]
        if key in self._defaults:
            return self._defaults[key]
        raise KeyError(key)

    def add_config_path(self, *args):
        for path in args:
            if path not in self.config_paths:
                self.config_paths.append(path)

    def check_config(self, prefix=''):
        if prefix:
            prefix = prefix + '/'
        for r in self._required:
            if r not in self:
                msg = "Required Configuration setting %s%s is missing!" % (prefix,r)
                logger.error(msg)
                raise ConfigurationError(msg)
        for k, item in self.items():
            if hasattr(item, 'check_config'):
                item.check_config(prefix="%s%s" % (prefix,k))

    @classmethod
    def _load_yaml_config(cls, config_data, filename="(unknown)"):
        """Load a yaml config file."""

        try:
            config = yaml.safe_load(config_data)
        except yaml.YAMLError as err:
            if hasattr(err, 'problem_mark'):
                mark = err.problem_mark
                errmsg = ("Invalid YAML syntax in Configuration file "
                            "%(file)s at line: %(line)s, column: %(column)s."
                          % dict(file=filename,
                                 line=mark.line + 1,
                                 column=mark.column + 1))
            else:
                errmsg = ("YAML error reading Configuration file "
                            "%(file)s"
                          % dict(file=filename))
            logger.error(errmsg)
            raise

        logger.info("Configuration: %s", config)
        return config

    @classmethod
    def _load_file(cls, filename, paths):
        for path in paths:
            fullpath = os.path.join(path, filename)
            if os.path.isfile(fullpath):
                with open(fullpath, 'r') as cf:
                    logger.debug("Loading configuration file: %s", fullpath)
                    return cf.read()
        msg = "Unable to find file %s in %s" % (filename, str(paths))
        logger.info(msg)
        return None

    @classmethod
    def load_config_file(cls, filename, filetype=None, paths=None):
        if not paths:
            paths = ['.']
        if filetype is None:
            if (filename.lower().endswith('.yaml') or
                filename.lower().endswith('.yml')):
                filetype = 'yaml'
            elif filename.lower().endswith('.json'):
                filetype = 'json'
            elif (filename.lower().endswith('.conf') or
                filename.lower().endswith('.ini')):
                filetype = 'ini'
            else:
                filetype = 'yaml'
        data = cls._load_file(filename, paths)
        if data is None:
            raise ConfigurationError("Cannot find or read config file: %s" % filename)
        try:
            loader = getattr(cls, "_load_%s_config" % filetype)
        except AttributeError:
            raise ConfigurationError("Unknown config file type: %s" % filetype)
        return loader(data, filename=filename)

    def load_file(self, filename, filetype=None):
        return self.load_config_file(filename, filetype, paths=self.config_paths)


