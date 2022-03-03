# from torchvision import transforms
import pytorch_lightning as pl

from model.MLP import MLP
from connections.elastic import Elastic
from dataset.rides import RideDataset
from dataset.data import Data

if __name__ == '__main__':
    elastic_connection = Elastic(host='https://routing-elasticsearch.apps.private.teh-1.snappcloud.io')
    rideDataset = RideDataset(elastic_connection=elastic_connection)
    train_rides = rideDataset.fetch_rides(time_least=1644610058, time_most=1644621158)
    val_rides = rideDataset.fetch_rides(time_least=1644621158, time_most=1644622158)

    pl.seed_everything(42)
    mlp = MLP(train_rides, val_rides)
    trainer = pl.Trainer(auto_scale_batch_size='power', gpus=0, deterministic=True, max_epochs=5)
    dm = Data(train_rides, val_rides)
    trainer.fit(mlp, dm)
    trainer.validate(mlp, dm)
