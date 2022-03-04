import pytorch_lightning as pl
from torch.utils.data import DataLoader


class Data(pl.LightningDataModule):
    def __init__(self, train, val, batch_size: int = 64):
        super().__init__()
        self.train = train
        self.val = val
        self.batch_size = batch_size

    def train_dataloader(self):
        return DataLoader(self.train, batch_size=self.batch_size, num_workers=8)

    def val_dataloader(self):
        return DataLoader(self.val, batch_size=self.batch_size, num_workers=8)
