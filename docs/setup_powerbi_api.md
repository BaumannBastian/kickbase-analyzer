# Power BI REST API Setup (Service Principal)

## Ziel
Power BI Service per API steuern (Workspaces, Datasets, Refreshes) ohne manuelle Klickstrecken.

## 1) Entra App Registration erstellen
1) Entra ID -> App registrations -> New registration
2) `Application (client) ID` und `Directory (tenant) ID` notieren
3) Unter `Certificates & secrets` ein Client Secret erstellen

## 2) Power BI Tenant fuer Service Principals vorbereiten
1) Power BI Admin Portal -> Tenant settings
2) Service principals fuer APIs aktivieren
3) Optional auf eine Security Group einschränken

## 3) Workspace-Berechtigung setzen
1) Im Ziel-Workspace den Service Principal als Member/Admin hinzufuegen
2) Danach sind Dataset/Refresh-Endpunkte fuer diesen Workspace nutzbar

## 4) Lokale Environment Variablen setzen
In `.env`:

```bash
POWERBI_TENANT_ID=<tenant_id>
POWERBI_CLIENT_ID=<client_id>
POWERBI_CLIENT_SECRET=<client_secret>
POWERBI_SCOPE=https://analysis.windows.net/powerbi/api/.default
POWERBI_WORKSPACE_ID=<workspace_id>
```

## 5) API Skripte ausfuehren
Workspaces listen:

```bash
./scripts/powerbi/run_powerbi_api.sh list-workspaces
```

Datasets eines Workspaces:

```bash
./scripts/powerbi/run_powerbi_api.sh list-datasets --workspace-id <workspace_id>
```

Refresh triggern:

```bash
./scripts/powerbi/run_powerbi_api.sh trigger-refresh --workspace-id <workspace_id> --dataset-id <dataset_id>
```

Refresh-Historie lesen:

```bash
./scripts/powerbi/run_powerbi_api.sh list-refreshes --workspace-id <workspace_id> --dataset-id <dataset_id>
```
