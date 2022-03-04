# from torchvision import transforms
import pytorch_lightning as pl

from model.MLP import MLP
from connections.elastic import Elastic
from dataset.rides import RideDataset
from dataset.data import Data
from torch.utils.tensorboard import SummaryWriter
import numpy as np

if __name__ == '__main__':
    elastic_connection = Elastic(host='https://routing-elasticsearch.apps.private.teh-1.snappcloud.io')
    rideDataset = RideDataset(elastic_connection=elastic_connection)
    train_rides, fuckMean = rideDataset.fetch_rides(time_least=1644610058, time_most=1644621158)
    val_rides, valFuckMean = rideDataset.fetch_rides(time_least=1644621158, time_most=1644622158)

    fuckMean = np.array(fuckMean)
    valFuckMean = np.array(valFuckMean)
    print(fuckMean.mean(axis=0))
    print(fuckMean.std(axis=0))

    writer = SummaryWriter()
    pl.seed_everything(42)
    mlp = MLP(writer)
    # starting from a batch size of 1 keeps doubling the batch size until an out-of-memory (OOM) error is encountered
    # auto_scale_batch_size = 'power', gpus = 0,
    trainer = pl.Trainer(deterministic=True, max_epochs=5)
    dm = Data(train_rides, val_rides)
    trainer.fit(mlp, dm)
    trainer.validate(mlp, dm)
    writer.flush()
