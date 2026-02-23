# ------------------------------------
# nn_models.py
#
# Dieses Modul enthaelt ein kompaktes PyTorch-Multitask-Netz fuer
# dieselben Ziele wie die sklearn-Hierarchie (Start, Einwechslung,
# Minuten, Rohpunkte, On-Top-Punkte).
#
# Outputs
# ------------------------------------
# 1) Trainiertes TorchModelBundle
# 2) Epochen-Historie mit Train/Valid-Loss
# 3) Vorhersagen im gleichen Format wie sklearn (vergleichbar)
#
# Usage
# ------------------------------------
# - cfg = TorchTrainingConfig(epochs=120, batch_size=256)
# - bundle = train_torch_multitask_model(X_train, y_train, X_valid, y_valid, config=cfg)
# - pred = predict_with_torch_bundle(bundle, X_valid, meta_frame=valid_frame)
# ------------------------------------

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


def _require_numpy() -> Any:
    try:
        import numpy as np
    except ModuleNotFoundError as exc:  # pragma: no cover - runtime guard
        raise RuntimeError("numpy ist nicht installiert. Bitte ML-Abhaengigkeiten installieren.") from exc
    return np


def _require_pandas() -> Any:
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:  # pragma: no cover - runtime guard
        raise RuntimeError(
            "pandas ist nicht installiert. Installiere ML-Abhaengigkeiten (pandas, numpy, torch)."
        ) from exc
    return pd


def _require_torch() -> tuple[Any, Any, Any, Any]:
    try:
        import torch
        import torch.nn as nn
        from torch.utils.data import DataLoader, TensorDataset
    except ModuleNotFoundError as exc:  # pragma: no cover - runtime guard
        raise RuntimeError(
            "torch ist nicht installiert. Bitte PyTorch installieren, um NN-Training auszufuehren."
        ) from exc
    return torch, nn, DataLoader, TensorDataset


@dataclass(frozen=True)
class TorchTrainingConfig:
    epochs: int = 120
    batch_size: int = 256
    learning_rate: float = 1e-3
    weight_decay: float = 1e-4
    hidden_dim: int = 256
    dropout: float = 0.20
    patience: int = 14
    seed: int = 42
    device: str = "cpu"
    classification_loss_weight: float = 1.0
    regression_loss_weight: float = 1.0


@dataclass(frozen=True)
class TorchEpochMetrics:
    epoch: int
    train_loss: float
    valid_loss: float


@dataclass(frozen=True)
class TorchModelBundle:
    model: Any
    config: TorchTrainingConfig
    history: list[TorchEpochMetrics]
    metadata: dict[str, Any]


def _build_network(input_dim: int, config: TorchTrainingConfig) -> Any:
    _, nn, _, _ = _require_torch()

    class KickbaseMultiTaskNetwork(nn.Module):
        def __init__(self) -> None:
            super().__init__()
            self.backbone = nn.Sequential(
                nn.Linear(input_dim, config.hidden_dim),
                nn.ReLU(),
                nn.Dropout(config.dropout),
                nn.Linear(config.hidden_dim, config.hidden_dim // 2),
                nn.ReLU(),
                nn.Dropout(config.dropout),
            )

            head_dim = max(16, config.hidden_dim // 2)
            self.head_start = nn.Sequential(nn.Linear(head_dim, 1))
            self.head_sub = nn.Sequential(nn.Linear(head_dim, 1))
            self.head_minutes = nn.Sequential(nn.Linear(head_dim, 1))
            self.head_raw = nn.Sequential(nn.Linear(head_dim, 1))
            self.head_on_top = nn.Sequential(nn.Linear(head_dim, 1))

        def forward(self, x: Any) -> dict[str, Any]:
            hidden = self.backbone(x)
            return {
                "start_logit": self.head_start(hidden).squeeze(-1),
                "sub_logit": self.head_sub(hidden).squeeze(-1),
                "minutes": self.head_minutes(hidden).squeeze(-1),
                "raw_points": self.head_raw(hidden).squeeze(-1),
                "on_top_points": self.head_on_top(hidden).squeeze(-1),
            }

    return KickbaseMultiTaskNetwork()


def _prepare_target_matrix(target_frame: Any) -> Any:
    np = _require_numpy()
    pd = _require_pandas()

    frame = target_frame.copy()
    required = [
        "target_started",
        "target_subbed_in",
        "target_minutes",
        "target_raw_points",
        "target_on_top_points",
    ]
    for col in required:
        if col not in frame.columns:
            frame[col] = 0.0
        frame[col] = pd.to_numeric(frame[col], errors="coerce").fillna(0.0)

    y = frame[required].to_numpy(dtype=np.float32)
    return y


def _set_torch_seed(seed: int) -> None:
    torch, _, _, _ = _require_torch()
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)


def train_torch_multitask_model(
    X_train: Any,
    y_train_frame: Any,
    X_valid: Any,
    y_valid_frame: Any,
    *,
    config: TorchTrainingConfig | None = None,
) -> TorchModelBundle:
    np = _require_numpy()
    torch, nn, DataLoader, TensorDataset = _require_torch()

    cfg = config or TorchTrainingConfig()
    _set_torch_seed(cfg.seed)

    X_train_np = np.asarray(X_train, dtype=np.float32)
    X_valid_np = np.asarray(X_valid, dtype=np.float32)
    y_train_np = _prepare_target_matrix(y_train_frame)
    y_valid_np = _prepare_target_matrix(y_valid_frame)

    if X_train_np.ndim != 2:
        raise ValueError("X_train muss 2-dimensional sein")
    if X_valid_np.ndim != 2:
        raise ValueError("X_valid muss 2-dimensional sein")

    train_dataset = TensorDataset(
        torch.tensor(X_train_np),
        torch.tensor(y_train_np),
    )
    valid_dataset = TensorDataset(
        torch.tensor(X_valid_np),
        torch.tensor(y_valid_np),
    )

    train_loader = DataLoader(train_dataset, batch_size=cfg.batch_size, shuffle=True)
    valid_loader = DataLoader(valid_dataset, batch_size=cfg.batch_size, shuffle=False)

    device = torch.device(cfg.device)
    model = _build_network(input_dim=X_train_np.shape[1], config=cfg).to(device)

    bce = nn.BCEWithLogitsLoss()
    mse = nn.MSELoss()

    optimizer = torch.optim.AdamW(
        model.parameters(),
        lr=cfg.learning_rate,
        weight_decay=cfg.weight_decay,
    )

    best_state = None
    best_valid = float("inf")
    stale_epochs = 0
    history: list[TorchEpochMetrics] = []

    for epoch in range(1, cfg.epochs + 1):
        model.train()
        train_losses: list[float] = []

        for batch_x, batch_y in train_loader:
            batch_x = batch_x.to(device)
            batch_y = batch_y.to(device)

            pred = model(batch_x)
            loss_cls = bce(pred["start_logit"], batch_y[:, 0]) + bce(pred["sub_logit"], batch_y[:, 1])
            loss_reg = (
                mse(pred["minutes"], batch_y[:, 2])
                + mse(pred["raw_points"], batch_y[:, 3])
                + mse(pred["on_top_points"], batch_y[:, 4])
            )
            loss = cfg.classification_loss_weight * loss_cls + cfg.regression_loss_weight * loss_reg

            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            train_losses.append(float(loss.detach().cpu().item()))

        model.eval()
        valid_losses: list[float] = []
        with torch.no_grad():
            for batch_x, batch_y in valid_loader:
                batch_x = batch_x.to(device)
                batch_y = batch_y.to(device)
                pred = model(batch_x)

                loss_cls = bce(pred["start_logit"], batch_y[:, 0]) + bce(pred["sub_logit"], batch_y[:, 1])
                loss_reg = (
                    mse(pred["minutes"], batch_y[:, 2])
                    + mse(pred["raw_points"], batch_y[:, 3])
                    + mse(pred["on_top_points"], batch_y[:, 4])
                )
                loss = cfg.classification_loss_weight * loss_cls + cfg.regression_loss_weight * loss_reg
                valid_losses.append(float(loss.detach().cpu().item()))

        train_loss = float(sum(train_losses) / max(len(train_losses), 1))
        valid_loss = float(sum(valid_losses) / max(len(valid_losses), 1))
        history.append(TorchEpochMetrics(epoch=epoch, train_loss=train_loss, valid_loss=valid_loss))

        if valid_loss < best_valid:
            best_valid = valid_loss
            best_state = {k: v.detach().cpu().clone() for k, v in model.state_dict().items()}
            stale_epochs = 0
        else:
            stale_epochs += 1
            if stale_epochs >= cfg.patience:
                break

    if best_state is not None:
        model.load_state_dict(best_state)

    metadata = {
        "epochs_trained": int(len(history)),
        "best_valid_loss": float(best_valid),
        "input_dim": int(X_train_np.shape[1]),
        "train_rows": int(X_train_np.shape[0]),
        "valid_rows": int(X_valid_np.shape[0]),
    }

    return TorchModelBundle(model=model, config=cfg, history=history, metadata=metadata)


def predict_with_torch_bundle(bundle: TorchModelBundle, X: Any, *, meta_frame: Any | None = None) -> Any:
    pd = _require_pandas()
    np = _require_numpy()
    torch, _, _, _ = _require_torch()

    X_np = np.asarray(X, dtype=np.float32)
    device = torch.device(bundle.config.device)

    bundle.model.eval()
    with torch.no_grad():
        tensor_x = torch.tensor(X_np).to(device)
        pred = bundle.model(tensor_x)

        start_probability = torch.sigmoid(pred["start_logit"]).cpu().numpy()
        sub_probability = torch.sigmoid(pred["sub_logit"]).cpu().numpy()
        expected_minutes = pred["minutes"].cpu().numpy()
        raw_points_if_full = pred["raw_points"].cpu().numpy()
        on_top_points_if_full = pred["on_top_points"].cpu().numpy()

    start_probability = np.clip(start_probability, 0.0, 0.99)
    sub_probability = np.clip(sub_probability, 0.0, 0.99)
    expected_minutes = np.clip(expected_minutes, 0.0, 110.0)
    raw_points_if_full = np.clip(raw_points_if_full, 0.0, None)
    on_top_points_if_full = np.clip(on_top_points_if_full, 0.0, None)

    full_points = raw_points_if_full + on_top_points_if_full
    minute_scale = np.clip(expected_minutes / 90.0, 0.0, 1.2)

    expected_raw_next = start_probability * raw_points_if_full * minute_scale + sub_probability * raw_points_if_full * 0.30
    expected_on_top_next = (
        start_probability * on_top_points_if_full * minute_scale
        + sub_probability * on_top_points_if_full * 0.30
    )

    out = pd.DataFrame(
        {
            "start_probability": start_probability,
            "sub_probability": sub_probability,
            "expected_minutes": expected_minutes,
            "raw_points_if_full": raw_points_if_full,
            "on_top_points_if_full": on_top_points_if_full,
            "expected_points_if_full": full_points,
            "expected_raw_points_next_match": expected_raw_next,
            "expected_on_top_points_next_match": expected_on_top_next,
            "expected_points_next_match": expected_raw_next + expected_on_top_next,
        }
    )

    if meta_frame is not None:
        for col in ["player_uid", "player_name", "team_uid", "match_uid", "snapshot_date"]:
            if col in meta_frame.columns:
                out[col] = meta_frame[col].to_numpy()

    return out
