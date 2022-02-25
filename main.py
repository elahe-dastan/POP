import os
# from torch.utils.data import DataLoader
# from torchvision import transforms
# import pytorch_lightning as pl

# from MLP import MLP
from connections.elastic import Elastic
from dataset.rides import RideDataset

if __name__ == '__main__':
    elastic_connection = Elastic(host='https://routing-elasticsearch.apps.private.teh-1.snappcloud.io')
    rideDataset = RideDataset(elastic_connection=elastic_connection)
    rides = rideDataset.fetch_rides(time_least=1644610058, time_most=1644613658)
    print(rides)
    # dataset = CIFAR10(os.getcwd(), download=True, transform=transforms.ToTensor())
    # pl.seed_everything(42)
    # mlp = MLP()
    # trainer = pl.Trainer(auto_scale_batch_size='power', gpus=0, deterministic=True, max_epochs=5)
    # trainer.fit(mlp, DataLoader(dataset))
