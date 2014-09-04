import argparse
import daemon
import logging
from logging.config import fileConfig


logger = logging.getLogger(__name__)


from winchester.config import ConfigManager
from winchester.pipeline_manager import PipelineManager


def main():
    parser = argparse.ArgumentParser(description="Winchester pipeline worker")
    parser.add_argument('--config', '-c', default='winchester.yaml',
                        help='The name of the winchester config file')
    parser.add_argument('--daemon', '-d', help='Run in daemon mode.')
    args = parser.parse_args()
    conf = ConfigManager.load_config_file(args.config)

    if 'logging_config' in conf:
        fileConfig(conf['logging_config'])
    else:
        logging.basicConfig()
        if 'log_level' in conf:
            level = conf['log_level']
            level = getattr(logging, level.upper())
            logging.getLogger('winchester').setLevel(level)
    pipe = PipelineManager(conf)
    if args.daemon:
        print "Backgrounding for daemon mode."
        with daemon.DaemonContext():
            pipe.run()
    else:
        pipe.run()


if __name__ == '__main__':
    main()
