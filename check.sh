#!/bin/bash

# Script de validación de código para SpaceNews Pipeline
# Ejecuta ruff check, ruff format, mypy y pytest

set -e  # Detener si algún comando falla

# Colores para output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "============================================="
echo "🔍 VALIDACIONES DE CÓDIGO - SpaceNews Pipeline"
echo "============================================="
echo ""

# 1. Ruff Check (linting)
echo -e "${YELLOW}[1/4] Ejecutando ruff check...${NC}"
if ruff check src/ dags/ glue/scripts/ tests/; then
    echo -e "${GREEN}✅ Ruff check pasó exitosamente${NC}"
else
    echo -e "${RED}❌ Ruff check encontró problemas${NC}"
    exit 1
fi
echo ""

# 2. Ruff Format (formateo)
echo -e "${YELLOW}[2/4] Ejecutando ruff format --check...${NC}"
if ruff format --check src/ dags/ glue/scripts/ tests/; then
    echo -e "${GREEN}✅ Formato correcto${NC}"
else
    echo -e "${RED}❌ Archivos necesitan formateo. Ejecuta: ruff format src/ dags/ glue/scripts/ tests/${NC}"
    exit 1
fi
echo ""

# 3. MyPy (type checking)
echo -e "${YELLOW}[3/4] Ejecutando mypy...${NC}"
if mypy src/ --ignore-missing-imports --no-strict-optional; then
    echo -e "${GREEN}✅ MyPy pasó exitosamente${NC}"
else
    echo -e "${RED}❌ MyPy encontró errores de tipos${NC}"
    exit 1
fi
echo ""

# 4. Pytest (tests)
echo -e "${YELLOW}[4/4] Ejecutando pytest...${NC}"
if pytest tests/ -v --tb=short --cov=src --cov-report=term-missing --cov-report=html; then
    echo -e "${GREEN}✅ Todos los tests pasaron${NC}"
    echo -e "${GREEN}   Reporte de cobertura: htmlcov/index.html${NC}"
else
    echo -e "${RED}❌ Algunos tests fallaron${NC}"
    exit 1
fi
echo ""

# Resumen final
echo "============================================="
echo -e "${GREEN}✅ TODAS LAS VALIDACIONES PASARON${NC}"
echo "============================================="
echo ""
echo "📊 Resumen:"
echo "  ✓ Ruff check (linting)"
echo "  ✓ Ruff format (código formateado)"
echo "  ✓ MyPy (tipado correcto)"
echo "  ✓ Pytest (todos los tests pasan)"
echo ""
