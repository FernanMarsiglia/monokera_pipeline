# 🚀 Deployment Guide - monokera-pipeline on EC2

Este documento describe cómo desplegar la pipeline en una EC2 con auto-deploy en cada push.

## 📋 Requisitos

- EC2 instance con Amazon Linux 2 o Ubuntu
- Mínimo: `t3.medium` (2 vCPU, 4GB RAM)
- Security Group abierto para SSH (puerto 22)
- Credenciales AWS (IAM user con permisos Glue, S3, Redshift)

## 🔧 Setup Inicial de la EC2

### 1. Conectarse a la EC2

```bash
ssh -i your-key.pem ec2-user@your-ec2-public-ip
```

### 2. Ejecutar script de setup

```bash
# Clone the repo to get the setup script
git clone https://github.com/FernanMarsiglia/monokera_pipeline.git
cd monokera_pipeline

# Run setup script
bash deploy/setup-ec2.sh
```

El script hará:
- ✅ Instalar Docker y Docker Compose
- ✅ Configurar SSH para GitHub
- ✅ Clonar el repositorio
- ✅ Crear archivo `.env` (debes completar credenciales)
- ✅ Iniciar servicios con Docker Compose

### 3. Configurar credenciales AWS

```bash
# En la EC2, editar el .env
nano .env

# O copiar desde tu máquina local
scp -i your-key.pem .env ec2-user@your-ec2-public-ip:/home/ec2-user/monokera_pipeline/
```

**Variables esenciales en `.env`:**
```env
AWS_ACCESS_KEY_ID=tu_access_key
AWS_SECRET_ACCESS_KEY=tu_secret_key
S3_BUCKET=monokera-bucket
GLUE_JOB_CLEAN_DEDUP=spacenews-01-clean-dedup
GLUE_JOB_ENRICH=spacenews-02-enrich-topics-entities
GLUE_JOB_TRENDS=spacenews-03-trends-aggregations
REDSHIFT_WORKGROUP=monokera-pipeline
LOCAL_MODE=false
```

## 🔐 Configurar Auto-Deploy con GitHub Actions

### 1. Generar SSH Key en la EC2

```bash
ssh -i your-key.pem ec2-user@your-ec2-public-ip

# En la EC2:
ssh-keygen -t ed25519 -C "github-deploy" -f ~/.ssh/github_deploy -N ""

# Ver la clave pública
cat ~/.ssh/github_deploy.pub
```

### 2. Agregar clave pública a GitHub

```
GitHub → Settings → SSH and GPG keys → New SSH Key
Title: EC2 Deploy Key
Paste el contenido de ~/.ssh/github_deploy.pub
```

### 3. Crear GitHub Secrets

En tu repositorio:
`Settings → Secrets and variables → Actions → New repository secret`

Crea estos secrets:

| Secret | Valor | Ejemplo |
|--------|-------|---------|
| `EC2_HOST` | IP pública de la EC2 | `54.123.45.67` |
| `EC2_USER` | Usuario SSH | `ec2-user` |
| `EC2_PRIVATE_KEY` | Contenido de `~/.ssh/github_deploy` | (privada) |
| `APP_PATH` | Ruta en EC2 | `/home/ec2-user/monokera_pipeline` |

**Obtener la clave privada:**

```bash
# En la EC2:
cat ~/.ssh/github_deploy
# Copiar TODO (incluyendo -----BEGIN... y -----END...)
```

### 4. Configuraración del SSH en la EC2 (opcional pero recomendado)

Para que el usuario `ec2-user` pueda hacer git pull sin contraseña:

```bash
ssh -i your-key.pem ec2-user@your-ec2-public-ip

# Agregar la clave SSH pública al authorized_keys
mkdir -p ~/.ssh
echo "$(cat ~/.ssh/github_deploy.pub)" >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
```

## 📊 Monitorear Deployment

### Ver logs de GitHub Actions

1. Ve a tu repo en GitHub
2. Click en `Actions`
3. Selecciona el workflow `Deploy to EC2`
4. Ver logs del último run

### Ver logs en la EC2

```bash
ssh -i your-key.pem ec2-user@your-ec2-public-ip

# Ver estado de containers
docker compose ps

# Ver logs de Airflow
docker compose logs -f airflow-scheduler

# Ver logs de última ejecución del DAG
docker compose logs --tail=100 airflow-scheduler | grep "spacenews_pipeline"
```

## 🔄 Flujo de Deployment

```
1. Local: git push origin main
   ↓
2. GitHub: Trigger workflow "Deploy to EC2"
   ↓
3. GitHub Actions: 
   - Clone repo
   - Setup SSH
   - SSH a EC2
   ↓
4. EC2:
   - git pull origin main
   - docker compose down --remove-orphans
   - docker compose up -d
   - Health check
   ↓
5. ✅ Pipeline lista para ejecutar
```

## 🧪 Probar el Deployment

### Primer deploy después del setup

```bash
# En tu máquina local
git add .
git commit -m "chore: test deployment"
git push origin main

# Ver en GitHub → Actions (debería empezar el deploy)
```

### Monitorear la ejecución en EC2

```bash
ssh -i your-key.pem ec2-user@your-ec2-public-ip
docker compose logs -f airflow-scheduler
```

## 🐛 Troubleshooting

### "Permission denied" en SSH

Verifica que la clave privada en `EC2_PRIVATE_KEY` sea codificada correctamente:
- No debe tener espacios al inicio
- Debe incluir `-----BEGIN OPENSSH PRIVATE KEY-----` y `-----END OPENSSH PRIVATE KEY-----`

### Docker no inicia

```bash
ssh -i your-key.pem ec2-user@your-ec2-public-ip
docker compose logs
# Aumenta memoria si es necesario
```

### Airflow no arranca

```bash
docker compose restart airflow-scheduler
docker compose logs airflow-scheduler --tail=50
```

### Git pull falla

```bash
# En EC2, verificar que SSH key está configurada
ssh -T git@github.com
# Debe mostrar: "Hi FernanMarsiglia! You've successfully authenticated..."
```

## 📈 Monitoreo Continuo

### Acceder a Airflow WebUI desde tu máquina

```bash
# Port forward desde EC2
ssh -i your-key.pem -L 8080:localhost:8080 ec2-user@your-ec2-public-ip

# Abrir en browser
# http://localhost:8080
```

### Ver CloudWatch logs de Glue

```bash
# En tu máquina local, instalar AWS CLI
aws configure

# Ver logs del último Glue job
aws logs tail /aws-glue/jobs/logs-v2 --follow
```

---

## ✅ Checklist de Setup Completo

- [ ] EC2 creada y accesible por SSH
- [ ] Docker y Docker Compose instalados
- [ ] Repositorio cloneado en `/home/ec2-user/monokera_pipeline`
- [ ] `.env` configurado con credenciales AWS
- [ ] SSH key de GitHub generada en EC2
- [ ] SSH public key agregada a GitHub
- [ ] GitHub Secrets configurados (`EC2_HOST`, `EC2_USER`, `EC2_PRIVATE_KEY`, `APP_PATH`)
- [ ] Docker Compose running (`docker compose ps` muestra containers up)
- [ ] Airflow accessible en `http://ec2-ip:8080`
- [ ] Test push realizado y deployment exitoso

---

¿Dudas? Revisa los logs con:
```bash
# GitHub Actions logs
echo "Check Actions tab in GitHub"

# EC2 logs
ssh -i your-key.pem ec2-user@your-ec2-ip "docker compose logs"
```
