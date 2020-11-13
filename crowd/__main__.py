import click
from .crowdapi import CrowdTangle


@click.command()
@click.option('-c','--config', nargs=1, default="default_query.yml")
@click.option('-a','--append', 'append', flag_value=True)
def main(config, append):
    ct = CrowdTangle(config, append)
@click.option('-c', '--config', nargs=1, default="default_query.yml")
def main(config):
    ct = CrowdTangle(config)
    ct.run()


if __name__ == '__main__':
    main()
