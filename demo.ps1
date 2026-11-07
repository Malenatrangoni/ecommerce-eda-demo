# ==============================================================================
# Script Interactivo de Demostracion en Vivo: EDA + Pub/Sub con RabbitMQ
# Universidad Tecnologica Nacional (UTN) - Sistemas Distribuidos
# ==============================================================================

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
$UrlBase = "http://localhost:3000"
$CredencialesRabbit = New-Object System.Management.Automation.PSCredential("guest", (ConvertTo-SecureString "guest" -AsPlainText -Force))

function Mostrar-Encabezado($titulo) {
    Write-Host "`n==========================================================================" -ForegroundColor Cyan
    Write-Host "  $titulo" -ForegroundColor Yellow
    Write-Host "==========================================================================" -ForegroundColor Cyan
}

function Mostrar-LogsRecientes($Desde, $Servicios = @("servicio-inventario", "servicio-facturacion", "servicio-envios", "servicio-notificaciones")) {
    Write-Host "`n--- Registros en vivo de los microservicios consumidores ---" -ForegroundColor DarkCyan
    docker compose logs --since $Desde @Servicios
}

function Escenario-1-PubSubConcurrente {
    Mostrar-Encabezado "ESCENARIO 1: Publicacion/Suscripcion, Coreografia y Evento Fat (ECST)"
    Write-Host "Se enviara un pedido de compra (POST /pedidos). El productor publicara 'PedidoCreado'"
    Write-Host "en el topico 'pedido.creado' llevando todo el estado (ECST), y los 4 consumidores"
    Write-Host "reaccionaran en paralelo y de forma autonoma sin bloquear al cliente.`n"

    $cuerpo = @{
        idPedido = "PED-UTN-101"
        cliente = @{
            nombre = "Estudiante UTN"
            correo = "alumno@utn.edu.ar"
            direccion = "French 414, Resistencia, Chaco"
        }
        articulos = @(
            @{ codigo = "NOTEBOOK-01"; nombre = "Notebook Desarrollo 16GB"; cantidad = 1; precio = 1200.00 },
            @{ codigo = "MOUSE-02"; nombre = "Mouse Inalambrico"; cantidad = 1; precio = 25.50 }
        )
        total = 1225.50
    } | ConvertTo-Json -Depth 5

    $inicio = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    $respuesta = Invoke-RestMethod -Uri "$UrlBase/pedidos" -Method Post -ContentType "application/json; charset=utf-8" -Body ([System.Text.Encoding]::UTF8.GetBytes($cuerpo))
    Write-Host "[OK] Respuesta HTTP inmediata del Productor (201 Created):" -ForegroundColor Green
    $respuesta | ConvertTo-Json -Depth 5 | Write-Host

    Start-Sleep -Seconds 2
    Mostrar-LogsRecientes $inicio
}

function Escenario-2-EnrutamientoSelectivoYEventoThin {
    Mostrar-Encabezado "ESCENARIO 2: Enrutamiento Selectivo por Topicos y Evento Ligero (Thin Event)"
    Write-Host "Se cancelara el pedido 'PED-UTN-101' (POST /pedidos/PED-UTN-101/cancelar)."
    Write-Host "El productor emite 'PedidoCancelado' en el topico 'pedido.cancelado' solo con el ID y motivo."
    Write-Host "-> Inventario, Facturacion y Notificaciones (pedido.#) LO RECIBEN."
    Write-Host "-> Servicio de Envios (suscrito solo a 'pedido.creado') NO LO RECIBE (filtrado en el broker).`n"

    $cuerpo = @{
        motivo = "El cliente decidio cambiar el medio de pago"
    } | ConvertTo-Json

    $inicio = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    $respuesta = Invoke-RestMethod -Uri "$UrlBase/pedidos/PED-UTN-101/cancelar" -Method Post -ContentType "application/json; charset=utf-8" -Body ([System.Text.Encoding]::UTF8.GetBytes($cuerpo))
    Write-Host "[OK] Respuesta HTTP inmediata del Productor:" -ForegroundColor Green
    $respuesta | ConvertTo-Json -Depth 5 | Write-Host

    Start-Sleep -Seconds 2
    Mostrar-LogsRecientes $inicio
}

function Escenario-3-DesacopleTemporalYRecuperacion {
    Mostrar-Encabezado "ESCENARIO 3: Desacople Temporal, Persistencia y Recuperacion ante Caida"
    Write-Host "1. Deteniendo el contenedor 'servicio-facturacion' (simulando caida del microservicio)..." -ForegroundColor Yellow
    docker compose stop servicio-facturacion

    Write-Host "`n2. Publicando nuevo pedido 'PED-CAIDA-202' mientras Facturacion esta fuera de linea..." -ForegroundColor Yellow
    $cuerpo = @{
        idPedido = "PED-CAIDA-202"
        cliente = @{
            nombre = "Maria Gomez"
            correo = "maria@utn.edu.ar"
            direccion = "Av. Sarmiento 850, Resistencia, Chaco"
        }
        articulos = @(
            @{ codigo = "MONITOR-24"; nombre = "Monitor IPS 24 Pulgadas"; cantidad = 2; precio = 300.00 }
        )
        total = 600.00
    } | ConvertTo-Json -Depth 5

    $respuesta = Invoke-RestMethod -Uri "$UrlBase/pedidos" -Method Post -ContentType "application/json; charset=utf-8" -Body ([System.Text.Encoding]::UTF8.GetBytes($cuerpo))
    Write-Host "[OK] El Productor respondio 201 Created sin verse afectado por la caida de Facturacion." -ForegroundColor Green

    Start-Sleep -Seconds 2
    $infoCola = Invoke-RestMethod -Uri "http://localhost:15672/api/queues/%2F/cola_facturacion" -Credential $CredencialesRabbit
    Write-Host "`n[RabbitMQ] Estado -> 'cola_facturacion' tiene [$($infoCola.messages)] mensaje(s) retenido(s) de forma segura esperando al consumidor." -ForegroundColor Magenta

    Write-Host "`n3. Iniciando nuevamente 'servicio-facturacion' para demostrar el procesamiento diferido..." -ForegroundColor Yellow
    $inicioRecuperacion = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    docker compose start servicio-facturacion
    Start-Sleep -Seconds 4

    Mostrar-LogsRecientes $inicioRecuperacion @("servicio-facturacion")
}

function Escenario-4-IdempotenciaYVersionado {
    Mostrar-Encabezado "ESCENARIO 4: Garantia At-Least-Once, Idempotencia y Control de Versiones"
    $idEventoFijo = "EVT-DUPLICADO-$(Get-Random -Minimum 100 -Maximum 999)"
    $idPedidoIdemp = "PED-IDEMP-$(Get-Random -Minimum 100 -Maximum 999)"
    Write-Host "Enviando el pedido '$idPedidoIdemp' por PRIMERA vez con idEvento='$idEventoFijo'..." -ForegroundColor Yellow

    $cuerpo = @{
        idEvento = $idEventoFijo
        idPedido = $idPedidoIdemp
        version = 1
        cliente = @{
            nombre = "Carlos Ruiz"
            correo = "carlos@utn.edu.ar"
            direccion = "9 de Julio 500, Corrientes"
        }
        articulos = @(
            @{ codigo = "TECLADO-MEC"; nombre = "Teclado Mecanico"; cantidad = 1; precio = 150.00 }
        )
        total = 150.00
    } | ConvertTo-Json -Depth 5

    $inicio = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    Invoke-RestMethod -Uri "$UrlBase/pedidos" -Method Post -ContentType "application/json; charset=utf-8" -Body ([System.Text.Encoding]::UTF8.GetBytes($cuerpo)) | Out-Null
    Start-Sleep -Seconds 2

    Write-Host "Reenviando EXACTAMENTE el mismo evento (idEvento='$idEventoFijo') simulando un reintento de red..." -ForegroundColor Yellow
    Invoke-RestMethod -Uri "$UrlBase/pedidos" -Method Post -ContentType "application/json; charset=utf-8" -Body ([System.Text.Encoding]::UTF8.GetBytes($cuerpo)) | Out-Null
    Start-Sleep -Seconds 2

    Mostrar-LogsRecientes $inicio
}

function Escenario-5-ColaMensajesMuertosDLQ {
    Mostrar-Encabezado "ESCENARIO 5: Tolerancia a Fallos y Cola de Mensajes Muertos (DLQ)"
    Write-Host "Enviando un pedido con datos invalidos / fallo simulado ('PED-ERROR-500', total: -50)..." -ForegroundColor Yellow
    Write-Host "Los consumidores rechazaran el mensaje (NACK sin reencolar) y RabbitMQ lo derivara"
    Write-Host "automaticamente hacia 'cola_mensajes_muertos' (DLQ) para auditoria sin bloquear las colas.`n"

    $cuerpo = @{
        idPedido = "PED-ERROR-$(Get-Random -Minimum 500 -Maximum 999)"
        simularFallo = $true
        cliente = @{
            nombre = "Prueba DLQ"
            correo = "error@utn.edu.ar"
            direccion = "Calle Falsa 123"
        }
        articulos = @(
            @{ codigo = "ITEM-ERR"; nombre = "Articulo Defectuoso"; cantidad = 1; precio = -50.00 }
        )
        total = -50.00
    } | ConvertTo-Json -Depth 5

    $inicio = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    Invoke-RestMethod -Uri "$UrlBase/pedidos" -Method Post -ContentType "application/json; charset=utf-8" -Body ([System.Text.Encoding]::UTF8.GetBytes($cuerpo)) | Out-Null
    Start-Sleep -Seconds 2

    $infoDLQ = Invoke-RestMethod -Uri "http://localhost:15672/api/queues/%2F/cola_mensajes_muertos" -Credential $CredencialesRabbit
    Write-Host "[DLQ] Estado en RabbitMQ -> 'cola_mensajes_muertos' acumula ahora [$($infoDLQ.messages)] mensaje(s) aislado(s)." -ForegroundColor Red

    Mostrar-LogsRecientes $inicio
}

if ($args.Count -gt 0) {
    switch ($args[0]) {
        "1" { Escenario-1-PubSubConcurrente }
        "2" { Escenario-2-EnrutamientoSelectivoYEventoThin }
        "3" { Escenario-3-DesacopleTemporalYRecuperacion }
        "4" { Escenario-4-IdempotenciaYVersionado }
        "5" { Escenario-5-ColaMensajesMuertosDLQ }
        "todos" {
            Escenario-1-PubSubConcurrente
            Escenario-2-EnrutamientoSelectivoYEventoThin
            Escenario-3-DesacopleTemporalYRecuperacion
            Escenario-4-IdempotenciaYVersionado
            Escenario-5-ColaMensajesMuertosDLQ
        }
    }
    exit 0
}

while ($true) {
    Write-Host "`n==========================================================================" -ForegroundColor Cyan
    Write-Host "   PANEL DE DEMOSTRACION EN VIVO - EDA y PUB/SUB (RabbitMQ + Node.js)" -ForegroundColor White
    Write-Host "==========================================================================" -ForegroundColor Cyan
    Write-Host "  [1] Flujo Feliz Pub/Sub Concurrente + Coreografia + Evento Fat (ECST)"
    Write-Host "  [2] Enrutamiento Selectivo por Topicos + Evento Ligero (Thin Event)"
    Write-Host "  [3] Desacople Temporal: Caida de Consumidor, Retencion y Recuperacion"
    Write-Host "  [4] Entrega At-Least-Once: Control de Idempotencia ante Duplicados"
    Write-Host "  [5] Aislamiento de Errores: Envio a Cola de Mensajes Muertos (DLQ)"
    Write-Host "  [6] Ejecutar los 5 escenarios en secuencia"
    Write-Host "  [0] Salir"
    Write-Host "--------------------------------------------------------------------------"
    $opcion = Read-Host "Seleccione una opcion (0-6)"

    switch ($opcion) {
        "1" { Escenario-1-PubSubConcurrente }
        "2" { Escenario-2-EnrutamientoSelectivoYEventoThin }
        "3" { Escenario-3-DesacopleTemporalYRecuperacion }
        "4" { Escenario-4-IdempotenciaYVersionado }
        "5" { Escenario-5-ColaMensajesMuertosDLQ }
        "6" {
            Escenario-1-PubSubConcurrente
            Escenario-2-EnrutamientoSelectivoYEventoThin
            Escenario-3-DesacopleTemporalYRecuperacion
            Escenario-4-IdempotenciaYVersionado
            Escenario-5-ColaMensajesMuertosDLQ
        }
        "0" { break }
        default { Write-Host "Opcion no valida." -ForegroundColor Red }
    }
}
