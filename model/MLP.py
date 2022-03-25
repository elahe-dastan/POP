from torch.optim import Adam
from torch import nn
import pytorch_lightning as pl
from torch.optim.lr_scheduler import ExponentialLR
from torch.utils.tensorboard import SummaryWriter


class MLP(pl.LightningModule):

    def __init__(self):
        super().__init__()
        self.layers = nn.Sequential(
            nn.Linear(5, 4),
            nn.ReLU(),
            nn.Linear(4, 3),
            nn.ReLU(),
            nn.Linear(3, 3),
            nn.ReLU(),
            nn.Linear(3, 2),
            nn.ReLU(),
            nn.Linear(2, 1)
        )
        self.mse = nn.MSELoss()
        self.writer = SummaryWriter()
        self.epoch = 0

    def forward(self, x):
        return self.layers(x.float())

    def training_step(self, batch, batch_idx):
        loss = self.step(batch)
        # self.log('train_loss', loss)
        # print('train_loss', loss)
        self.writer.add_scalar("Loss/train", loss, self.epoch)
        self.writer.flush()
        self.epoch += 1

        return loss

    def validation_step(self, val_batch, batch_idx):
        loss = self.step(val_batch)
        print('val_loss', loss)
        # self.log('val_loss', loss)

    def step(self, batch):
        x, y = batch
        x = x.view(x.size(0), -1)
        y = y.view(y.size(0), 1)
        y_hat = self.layers(x.float())
        loss = self.mse(y_hat, y.float())

        return loss

    def configure_optimizers(self):
        optimizer = Adam(self.parameters(), lr=1e-4)
        scheduler = ExponentialLR(optimizer, gamma=0.9)

        return [optimizer], [scheduler]
