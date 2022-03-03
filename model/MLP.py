import os
import torch
from torch import nn
# from torchvision import transforms
import pytorch_lightning as pl


class MLP(pl.LightningModule):

    def __init__(self, train_rides, val_rides):
        super().__init__()
        self.layers = nn.Sequential(
            nn.Linear(5, 4),
            nn.ReLU(),
            nn.Linear(4, 3),
            nn.ReLU(),
            nn.Linear(3, 2),
            nn.ReLU(),
            nn.Linear(2, 1)
        )
        self.mse = nn.MSELoss()
        self.train_rides = train_rides
        self.val_rides = val_rides

    def forward(self, x):
        return self.layers(x)

    def training_step(self, batch, batch_idx):
        loss = self.step(batch)
        self.log('train_loss', loss)
        return loss

    def validation_step(self, val_batch, batch_idx):
        loss = self.step(val_batch)
        print('val_loss', loss)
        self.log('val_loss', loss)

    def step(self, batch):
        x, y = batch
        x = x.view(x.size(0), -1)
        y = y.view(y.size(0), 1)
        y_hat = self.layers(x.float())
        loss = self.mse(y_hat, y.float())
        return loss

    # def backward(self, trainer, loss, optimizer, optimizer_idx):
    #     loss.backward()

    def configure_optimizers(self):
        optimizer = torch.optim.Adam(self.parameters(), lr=1e-4)
        return optimizer
