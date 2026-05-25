#!/usr/bin/env python3

# start_worker_instances.py
import time, os, sys, subprocess
from os import environ as env
from novaclient import client
from keystoneauth1 import loading, session

# PRIVATE_KEY_PATH: private key used to connect to master VM
if len(sys.argv) != 2 or sys.argv[1] == '--help':
    print("Usage: python3 start_worker_instances.py <PRIVATE_KEY_PATH>")
    exit(1)

KEY_PATH = sys.argv[1]

# --- Read master info written by start_master_instance.py ---
MASTER_INFO_PATH = 'master_info.txt'
if not os.path.isfile(MASTER_INFO_PATH):
    sys.exit(f"'{MASTER_INFO_PATH}' not found. Run start_master_instance.py first.")

master_info = {}
with open(MASTER_INFO_PATH) as f:
    for line in f:
        line = line.strip()
        if '=' in line:
            key, _, value = line.partition('=')
            master_info[key.strip()] = value.strip()

MASTER_IP      = master_info.get('MASTER_IP')
CLUSTER_PUBKEY = master_info.get('CLUSTER_PUBKEY')

if not MASTER_IP or not CLUSTER_PUBKEY:
    sys.exit("master_info.txt is missing MASTER_IP or CLUSTER_PUBKEY. Re-run start_master_instance.py.")

print(f"Master IP:     {MASTER_IP}")
print(f"Cluster key:   {CLUSTER_PUBKEY[:40]}...")

# --- Config ---
FLAVOR_NAME  = 'ssc.medium'
PRIVATE_NET  = 'UPPMAX 2026/1-24 Internal IPv4 Network'
IMAGE_NAME   = 'Ubuntu 22.04 - 2024.01.15'
CLOUD_CFG_TEMPLATE_PATH = 'cloud-cfg-worker.txt'

# --- Auth ---
loader = loading.get_plugin_loader('password')
auth = loader.load_from_options(
    auth_url=env['OS_AUTH_URL'],
    username=env['OS_USERNAME'],
    password=env['OS_PASSWORD'],
    project_name=env['OS_PROJECT_NAME'],
    project_domain_id=env['OS_PROJECT_DOMAIN_ID'],
    project_id=env['OS_PROJECT_ID'],
    user_domain_name=env['OS_USER_DOMAIN_NAME'],
)
sess = session.Session(auth=auth)
nova = client.Client('2.1', session=sess)
print("Authenticated.")

# --- Resolve resources ---
image  = nova.glance.find_image(IMAGE_NAME)
flavor = nova.flavors.find(name=FLAVOR_NAME)
net    = nova.neutron.find_network(PRIVATE_NET)
nics   = [{'net-id': net.id}]

# Read the worker cloud-cfg template and inject the cluster public key
with open(CLOUD_CFG_TEMPLATE_PATH) as f:
    worker_cfg_template = f.read()

worker_userdata = worker_cfg_template.replace('__CLUSTER_PUBKEY__', CLUSTER_PUBKEY)

# --- Create worker instances ---
NUM_WORKERS = 4
instances = []
for i in range(1, NUM_WORKERS + 1):
    name = f'group16-worker-{i}'
    print(f"Creating instance '{name}'...")
    instance = nova.servers.create(
        name=name,
        image=image,
        flavor=flavor,
        userdata=worker_userdata,
        nics=nics,
        security_groups=['default'],
    )
    instances.append(instance)

# --- Wait for all workers to become ACTIVE ---
print(f"Waiting for {NUM_WORKERS} worker(s) to become ACTIVE...")
active_instances = []
for instance in instances:
    while True:
        instance = nova.servers.get(instance.id)
        if instance.status == 'ACTIVE':
            # Extract the internal IP from the network info
            addresses = list(instance.addresses.values())
            internal_ip = addresses[0][0]['addr'] if addresses else 'unknown'
            print(f"  {instance.name} is ACTIVE - internal IP: {internal_ip}")
            active_instances.append((instance.name, internal_ip))
            break
        elif instance.status == 'ERROR':
            print(f"  {instance.name} entered ERROR state, skipping.")
            break
        print(f"  {instance.name}: {instance.status}, waiting 5s...")
        time.sleep(5)

# --- Update /etc/hosts and ~/.ssh/config on master ---
print("Configuring master SSH access to workers...")

hosts_entries = '\n'.join(
    f'{ip}  {name}' for i, (name, ip) in enumerate(active_instances)
)
ssh_config_entries = '\n'.join(
    f'Host {name}\n    HostName {ip}\n    User ubuntu\n    IdentityFile ~/.ssh/cluster_key\n    StrictHostKeyChecking accept-new'
    for i, (name, ip) in enumerate(active_instances)
)

remote_cmd = f'''
echo "{hosts_entries}" | sudo tee -a /etc/hosts
echo "{ssh_config_entries}" >> /home/ubuntu/.ssh/config
'''

result = subprocess.run([
    'ssh', '-i', KEY_PATH, '-o', 'StrictHostKeyChecking=accept-new',
    f'ubuntu@{MASTER_IP}',
    remote_cmd
], capture_output=True, text=True)

if result.returncode == 0:
    print("  Done. From the master you can now use: ssh w1, ssh w2, ...")
else:
    print(f"  Warning: {result.stderr}")

# --- Print summary ---
print(f"\nDone. {len(active_instances)} worker(s) created:")
for name, ip in active_instances:
    print(f"  {name}  {ip}")

with open('workers_info.txt', 'w') as f:
    for name, ip in active_instances:
        f.write(f"{name}:{ip}\n")

print(f"\nTo SSH onto a worker from the master:")
print(f"  ssh -i ~/.ssh/cluster_key ubuntu@<worker-internal-ip>")
