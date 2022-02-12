import os
import torch
from torch import nn
from torch.utils.data import DataLoader
# from torchvision import transforms
import pytorch_lightning as pl


class MLP(pl.LightningModule):

    def __init__(self):
        super().__init__()
        self.layers = nn.Sequential(
            nn.Linear(32 * 32 * 3, 64),
            nn.ReLU(),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, 1)
        )
        self.mse = nn.MSELoss()

    def forward(self, x):
        return self.layers(x)

    def training_step(self, batch, batch_idx):
        x, y = batch
        x = x.view(x.size(0), -1)
        y_hat = self.layers(x)
        loss = self.mse(y_hat, y)
        self.log('train_loss', loss)
        return loss

    def configure_optimizers(self):
        optimizer = torch.optim.Adam(self.parameters(), lr=1e-4)
        return optimizer

