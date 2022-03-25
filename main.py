# from torchvision import transforms
import pytorch_lightning as pl

from model.MLP import MLP
from connections.elastic import Elastic
from dataset.rides import RideDataset
from dataset.data import Data
import torch


def retrieve_data():
    elastic_connection = Elastic(host='https://routing-elasticsearch.apps.private.teh-1.snappcloud.io')
    rideDataset = RideDataset(elastic_connection=elastic_connection)
    train_rides, mean, std = rideDataset.fetch_rides(time_least=1647761058, time_most=1647891058)
    val_rides = rideDataset.fetch_rides(time_least=1647761058, time_most=1647891058, mean=mean, std=std)

    return train_rides, val_rides, mean, std


def initiate_model(train_rides, val_rides):
    pl.seed_everything(42)
    mlp = MLP()
    # starting from a batch size of 1 keeps doubling the batch size until an out-of-memory (OOM) error is encountered
    # auto_scale_batch_size = 'power', gpus = 0,
    trainer = pl.Trainer(accelerator="gpu", devices=1, deterministic=True, max_epochs=200)
    dm = Data(train_rides, val_rides)
    trainer.fit(mlp, dm)
    trainer.validate(mlp, dm)

    return mlp

    # writer.close()


def load_model(model, mean, std):
    # Yt_train = torch.tensor([239., 0., 20.25, 27067901951., 27165943807.])
    Yt_train = torch.tensor([2045., 2., 22.9, 7731898367., 11101759487.])
    print("mean")
    print(mean[0:-1])
    Yt_train = (Yt_train - mean[0:-1]) / std[0:-1]
    y = model(Yt_train)
    y = y * std[-1] + mean[-1]
    print("estimated y")
    print(y)
    print("real y")
    print(1556)


if __name__ == '__main__':
    train_rides, val_rides, mean, std = retrieve_data()
    model = initiate_model(train_rides, val_rides)
    # model = MLP.load_from_checkpoint("./lightning_logs/version_0/checkpoints/epoch=199-step=15599.ckpt")
    load_model(model, mean, std)
