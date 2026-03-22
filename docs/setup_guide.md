# Setup Guide

## 1. GCP Project & Environment

1. Create a GCP account and project at [console.cloud.google.com](https://console.cloud.google.com)
2. Enable billing
3. Install [Google Cloud CLI](https://cloud.google.com/sdk/docs/install) and authenticate:

```bash
gcloud auth login
gcloud config set project <YOUR_PROJECT_ID>
```

---

## 2. GCS Bucket

1. Create bucket `raw_glamira` (Region: `asia-southeast1`, Storage class: Standard)
2. Upload raw data:

```bash
gcloud storage cp ./glamira_ubl_oct2019_nov2019.tar.gz gs://raw_glamira/
gcloud storage cp ./IP-COUNTRY-REGION-CITY.BIN gs://raw_glamira/
```

---

## 3. VM Setup

### Create VM

Create a VM instance in GCP Console (Compute Engine → VM instances → Create):

| Field | Value |
|---|---|
| Name | `glamira-vm` |
| Zone | `us-central1-a` |
| Machine type | `e2-standard-2` (2 vCPU, 8 GB RAM) |
| Boot disk | Ubuntu 22.04 LTS, 50 GB |

> **Zone note:** Test zone accessibility before creating the VM - some zones may block Glamira website traffic. Create a temporary `e2-micro` VM and run `curl -I https://www.glamira.com`. Use a zone that returns `200 OK`.

### Reserve Static IP

GCP Console → VPC Network → IP addresses → External IP addresses → find the VM's ephemeral IP → **Promote to static address**.

> Without a static IP, the address changes on every VM restart.

### Firewall Rule for MongoDB VM

GCP Console → VPC Network → Firewall → Create Firewall Rule:

| Field | Value |
|---|---|
| Name | `allow-mongodb-local` |
| Direction | Ingress |
| Action | Allow |
| Source IP ranges | `<your-local-ip>/32` |
| Protocols and ports | TCP: `27017` |

Get your local IP:
```bash
curl -4 ifconfig.me
```

> Use `/32` to allow exactly one IP. Never use `0.0.0.0/0`.

---

## 4. MongoDB Setup

SSH into the VM:
```bash
gcloud compute ssh glamira-vm --zone us-central1-a
```

### Install tmux (required for long-running tasks)

```bash
sudo apt install tmux
```

| Command | Action |
|---|---|
| `tmux new -s main` | Create new session |
| `Ctrl+B D` | Detach (session keeps running) |
| `tmux attach -t main` | Reattach |

### Install MongoDB 7.0

> MongoDB 8.0 is incompatible with Ubuntu 22.04 (`libc6 >= 2.38` required). Use 7.0.

```bash
sudo rm -f /etc/apt/sources.list.d/mongodb-org*.list

curl -fsSL https://www.mongodb.org/static/pgp/server-7.0.asc | \
  sudo gpg -o /usr/share/keyrings/mongodb-server-7.0.gpg --dearmor

echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] \
  https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" | \
  sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list

sudo apt-get update
sudo apt-get install -y mongodb-org
sudo systemctl enable mongod && sudo systemctl start mongod
```

### Create Admin User

> Create the user **before** enabling auth - otherwise you'll be locked out.

```bash
mongosh
```

```js
use admin
db.createUser({
  user: "admin",
  pwd: "<your-password>",
  roles: ["root"]
})
exit
```

### Enable Auth & External Access

Edit `/etc/mongod.conf`:

```yaml
net:
  port: 27017
  bindIp: 0.0.0.0

security:
  authorization: enabled
```

```bash
sudo systemctl restart mongod
```

### Test Connection

From local (MongoDB Compass or DataGrip):
```
mongodb://admin:<password>@<STATIC_IP>:27017/?authSource=admin
```

---

## 5. Data Import

Pull dataset from GCS and import into MongoDB (run in tmux — takes 30-60 min):

```bash
tmux new -s import

gcloud storage cp gs://raw_glamira/glamira_ubl_oct2019_nov2019.tar.gz ~/data/
cd ~/data && tar -xzf glamira_ubl_oct2019_nov2019.tar.gz

mongorestore --uri="mongodb://admin:<password>@localhost:27017/?authSource=admin" \
  --db countly dump/countly/
```

Verify:
```js
use countly
db.summary.estimatedDocumentCount()  // Expected: ~41,432,473
```

---

## 6. Local Project Setup

```bash
git clone https://github.com/plat102/ecom-analytics-pipeline.git
cd ecom-analytics-pipeline

# Requires Python 3.12+
poetry install

cp .env.example .env
# Fill in MONGO_URI and other variables
```

---

## Troubleshooting

**MongoDB install fails** (`libc6` dependency error)
Use MongoDB 7.0 as shown above, not 8.0.

**Cannot connect from local**
Check in order:
- firewall rule IP matches `curl -4 ifconfig.me`
- `bindIp: 0.0.0.0` in mongod.conf
- `sudo systemctl status mongod`
- Static IP still assigned to VM.

**Authentication error**
- `mongosh -u admin -p <password> --authenticationDatabase admin`
