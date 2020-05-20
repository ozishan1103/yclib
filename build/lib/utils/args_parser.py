# pylint:disable=unused-variable
import argparse


def parse_cli_args():
    """
    Parse command line argument .

    :return: parsed args dict
    """
    parser = argparse.ArgumentParser(description='Parse args')
    parser.add_argument('--pipeline', dest="pipeline", required=True, type=str,
                        help="Name of the pipeline")
    parser.add_argument('--job_type', dest="job_type", required=True, type=str,
                        help="Type of the job, transform/validate")
    parser.add_argument('--config', dest="config", required=True, type=str,
                        help="Config file to use for this job")

    known_args, unknown_args = parser.parse_known_args()
    known_args_dict = vars(known_args)

    return known_args_dict
