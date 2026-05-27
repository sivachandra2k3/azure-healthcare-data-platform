// =============================================================
// main.bicep - Azure Healthcare Data Platform Infrastructure
// Deploys: ADLS Gen2, ADF, Databricks, Synapse, Key Vault
// =============================================================

targetScope = 'resourceGroup'

@description('Environment: dev, staging, prod')
@allowed(['dev', 'staging', 'prod'])
param env string = 'dev'

@description('Azure region for all resources')
param location string = resourceGroup().location

@description('Project prefix for naming')
param projectPrefix string = 'healthcare'

@description('Admin object ID for Key Vault access')
param adminObjectId string

// ── Variables ──────────────────────────────────────────────────
var suffix          = '${projectPrefix}${env}'
var storageAccName  = 'adls${suffix}'
var adfName         = 'adf-${suffix}'
var databricksName  = 'dbw-${suffix}'
var synapseName     = 'synw-${suffix}'
var keyVaultName    = 'kv-${suffix}'
var logWorkspaceName= 'log-${suffix}'

var tags = {
  Environment : env
  Project     : 'Healthcare Data Platform'
  ManagedBy   : 'Bicep'
}

// ── Log Analytics Workspace ────────────────────────────────────
resource logWorkspace 'Microsoft.OperationalInsights/workspaces@2022-10-01' = {
  name     : logWorkspaceName
  location : location
  tags     : tags
  properties: {
    sku             : { name: 'PerGB2018' }
    retentionInDays : 30
  }
}

// ── Key Vault ──────────────────────────────────────────────────
resource keyVault 'Microsoft.KeyVault/vaults@2023-02-01' = {
  name     : keyVaultName
  location : location
  tags     : tags
  properties: {
    sku                    : { family: 'A', name: 'standard' }
    tenantId               : subscription().tenantId
    enabledForDeployment   : false
    enabledForTemplateDeployment: true
    enableSoftDelete       : true
    softDeleteRetentionInDays: 90
    enableRbacAuthorization: true
    networkAcls: {
      defaultAction : 'Deny'
      bypass        : 'AzureServices'
    }
    accessPolicies: [
      {
        tenantId   : subscription().tenantId
        objectId   : adminObjectId
        permissions: {
          secrets: ['get', 'list', 'set', 'delete']
          keys   : ['get', 'list', 'create', 'delete']
        }
      }
    ]
  }
}

// ── ADLS Gen2 ──────────────────────────────────────────────────
resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name     : storageAccName
  location : location
  tags     : tags
  sku      : { name: env == 'prod' ? 'Standard_ZRS' : 'Standard_LRS' }
  kind     : 'StorageV2'
  properties: {
    isHnsEnabled            : true           // Hierarchical namespace for ADLS Gen2
    minimumTlsVersion       : 'TLS1_2'
    supportsHttpsTrafficOnly: true
    allowBlobPublicAccess   : false
    networkAcls: {
      defaultAction : 'Deny'
      bypass        : 'AzureServices'
    }
  }
}

// Containers: bronze, silver, gold, models, logs
var containers = ['bronze', 'silver', 'gold', 'models', 'logs']

resource blobContainers 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = [
  for container in containers: {
    name: '${storageAccount.name}/default/${container}'
    properties: {
      publicAccess: 'None'
    }
  }
]

// ── Azure Data Factory ─────────────────────────────────────────
resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name     : adfName
  location : location
  tags     : tags
  identity : { type: 'SystemAssigned' }
  properties: {
    globalParameters: {
      environment: { type: 'String', value: env }
      storageAccount: { type: 'String', value: storageAccName }
    }
  }
}

// ADF diagnostic settings
resource adfDiagnostics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name  : 'adf-diagnostics'
  scope : dataFactory
  properties: {
    workspaceId: logWorkspace.id
    logs: [
      { category: 'PipelineRuns', enabled: true }
      { category: 'ActivityRuns', enabled: true }
      { category: 'TriggerRuns',  enabled: true }
    ]
    metrics: [
      { category: 'AllMetrics', enabled: true }
    ]
  }
}

// ── Azure Databricks ───────────────────────────────────────────
resource databricks 'Microsoft.Databricks/workspaces@2023-02-01' = {
  name     : databricksName
  location : location
  tags     : tags
  sku      : { name: env == 'prod' ? 'premium' : 'standard' }
  properties: {
    managedResourceGroupId: '${subscription().id}/resourceGroups/rg-${databricksName}-managed'
    parameters: {
      enableNoPublicIp: { value: env == 'prod' }
    }
  }
}

// ── Azure Synapse Analytics ────────────────────────────────────
resource synapse 'Microsoft.Synapse/workspaces@2021-06-01' = {
  name     : synapseName
  location : location
  tags     : tags
  identity : { type: 'SystemAssigned' }
  properties: {
    defaultDataLakeStorage: {
      accountUrl    : storageAccount.properties.primaryEndpoints.dfs
      filesystem    : 'gold'
      resourceId    : storageAccount.id
      createManagedPrivateEndpoint: false
    }
    sqlAdministratorLogin        : 'sqladminuser'
    sqlAdministratorLoginPassword: '@Microsoft.KeyVault(SecretUri=${keyVault.properties.vaultUri}secrets/synapse-sql-password/)'
    publicNetworkAccess          : env == 'prod' ? 'Disabled' : 'Enabled'
  }
}

// Synapse Serverless SQL Pool
resource synapseSqlPool 'Microsoft.Synapse/workspaces/sqlPools@2021-06-01' = {
  parent: synapse
  name  : 'synhealthcarepool'
  location: location
  tags  : tags
  sku   : { name: 'DW100c' }
  properties: {
    collation: 'SQL_Latin1_General_CP1_CI_AS'
  }
}

// ── RBAC Assignments ───────────────────────────────────────────

// ADF → Storage: Storage Blob Data Contributor
resource adfStorageRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name : guid(dataFactory.id, storageAccount.id, 'StorageBlobDataContributor')
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId     : dataFactory.identity.principalId
    principalType   : 'ServicePrincipal'
  }
}

// Synapse → Storage: Storage Blob Data Contributor
resource synapseStorageRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name : guid(synapse.id, storageAccount.id, 'StorageBlobDataContributor')
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId     : synapse.identity.principalId
    principalType   : 'ServicePrincipal'
  }
}

// ── Outputs ────────────────────────────────────────────────────
output storageAccountName  string = storageAccount.name
output dataFactoryName     string = dataFactory.name
output databricksWorkspace string = databricks.properties.workspaceUrl
output synapseWorkspace    string = synapse.name
output keyVaultUri         string = keyVault.properties.vaultUri
