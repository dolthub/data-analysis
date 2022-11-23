terraform {
  required_providers {
    digitalocean = {
      source  = "digitalocean/digitalocean"
      version = "~> 2.0"
    }
  }
}

provider "digitalocean" {
  token = file("do_token.txt")
}

data "digitalocean_ssh_keys" "keys" {
  sort {
    key       = "name"
    direction = "asc"
  }
}

resource "digitalocean_droplet" "server" {
  image     = "debian-11-x64"
  name      = "dolt-jupyter-housingprices"
  region    = "syd1"
  size      = "m-8vcpu-64gb"
  ssh_keys  = [ for key in data.digitalocean_ssh_keys.keys.ssh_keys: key.fingerprint ]
  user_data = file("provision.sh")
}

output "server_ip" {
  value = resource.digitalocean_droplet.server.ipv4_address
}

