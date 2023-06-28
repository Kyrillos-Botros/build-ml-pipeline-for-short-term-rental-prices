import pandas as pd
import logging
import wandb
import argparse
import tempfile
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger()

def go(args):

    #Initializing the connection with wandb
    logger.info("Initializing the connection with wandb")
    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Creating ouput artifact
    logger.info("Creating output artifact")
    output_artifact = wandb.Artifact(
        name=args.output_artifact,
        type=args.output_type,
        description=args.output_description
    )

    # Download the raw data file from wandb
    logger.info("Downloading the raw data file")
    data_file_path = wandb.use_artifact(args.input_artifact).file()
    df = pd.read_csv(data_file_path)

    # Removing the outliers
    logger.info("Removing the outliers")
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()

    # Convert last_review to datetime
    logger.info("Converting lst_review col to be in datetime format")
    df['last_review'] = pd.to_datetime(df['last_review'])

    # Create a temporary directory to save the output artifact
    with tempfile.TemporaryDirectory() as tmp_dir:
        temp_path = os.path.join(tmp_dir, args.output_artifact)
        df.to_csv(temp_path, index=False)
        output_artifact.add_file(temp_path)

        logger.info("Uploading the artifact to wandb")
        run.log_artifact(output_artifact)

        # wait till the artifact be uploaded
        output_artifact.wait()




if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Preprocessed data")
    parser.add_argument(
        "--input_artifact",
        type=str,
        help="This is the raw data file path",
        required=True
    )
    parser.add_argument(
        "--output_artifact",
        type=str,
        help="The name of the output artifact to be used",
        required=True
    )

    parser.add_argument(
        "--output_type",
        type=str,
        help="Type for the artifact",
        required=True
    )

    parser.add_argument(
        "--output_description",
        type=str,
        help="Description for the artifact",
        required=True,
    )

    parser.add_argument(
        "--min_price",
        type=float,
        help="The minimum price to be considered in outliers removal",
        required=True,
    )

    parser.add_argument(
        "--max_price",
        type=float,
        help="The maximum price to be considered in outliers removal",
        required=True,
    )

    args = parser.parse_args()
    go(args)