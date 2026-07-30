// Reference only. Requires an existing private workload-profile environment.
param location string = resourceGroup().location
param environmentId string
param name string = 'favn-control-plane'
param image string
param workspaceIds string
param runnerPools string
param viewPublicOrigin string
param viewTrustedProxyCidrs string
param enableEntraEasyAuth bool = false
param viewEntraTenantId string = ''
param viewEntraClientId string = ''
param viewEntraWorkspaceId string = ''
@allowed([
  'replace'
  'append'
  'ignore'
])
param viewForwardedForPolicy string = 'replace'
param runtimeInputPinKeyVersion int = 1
@secure()
param databaseUrl string
@secure()
param databaseCaCertificate string
@secure()
param runtimeInputPinKeys string
@secure()
param platformServiceTokens string
@secure()
param capacityReaderToken string
@secure()
param viewSecretKeyBase string
@secure()
param viewEntraClientSecret string = ''
@secure()
param distributionCookie string
@secure()
param distributionTlsOptions string
@secure()
param distributionCaCertificate string
@secure()
param distributionCertificate string
@secure()
param distributionPrivateKey string

resource controlPlane 'Microsoft.App/containerApps@2026-01-01' = {
  name: name
  location: location
  properties: {
    managedEnvironmentId: environmentId
    configuration: {
      activeRevisionsMode: 'Single'
      ingress: {
        external: false
        targetPort: 4101
        transport: 'http'
        allowInsecure: false
        additionalPortMappings: [
          {
            external: false
            targetPort: 4369
            exposedPort: 4369
          }
          {
            external: false
            targetPort: 9100
            exposedPort: 9100
          }
        ]
      }
      secrets: concat([
        {
          name: 'distribution-cookie'
          value: distributionCookie
        }
        {
          name: 'distribution-tls-options'
          value: distributionTlsOptions
        }
        {
          name: 'distribution-ca'
          value: distributionCaCertificate
        }
        {
          name: 'distribution-certificate'
          value: distributionCertificate
        }
        {
          name: 'distribution-private-key'
          value: distributionPrivateKey
        }
        {
          name: 'database-url'
          value: databaseUrl
        }
        {
          name: 'database-ca'
          value: databaseCaCertificate
        }
        {
          name: 'runtime-input-pin-keys'
          value: runtimeInputPinKeys
        }
        {
          name: 'api-service-tokens'
          // Both inputs are secure parameters; interpolation preserves them as
          // ARM expressions even though the linter cannot propagate that taint.
          #disable-next-line use-secure-value-for-secure-inputs
          value: '${platformServiceTokens},capacity-scaler|capacity_reader:${capacityReaderToken}'
        }
        {
          name: 'view-secret-key-base'
          value: viewSecretKeyBase
        }
      ], enableEntraEasyAuth ? [
        {
          name: 'entra-client-secret'
          value: viewEntraClientSecret
        }
      ] : [])
    }
    template: {
      containers: [
        {
          name: 'control-plane'
          image: image
          env: [
            {
              name: 'FAVN_DEPLOYMENT_MODE'
              value: 'production'
            }
            {
              name: 'FAVN_DATABASE_URL'
              secretRef: 'database-url'
            }
            {
              name: 'FAVN_DATABASE_SSL_MODE'
              value: 'verify-full'
            }
            {
              name: 'FAVN_DATABASE_SSL_CA_FILE'
              value: '/etc/favn/database-ca.crt'
            }
            {
              name: 'FAVN_RUNTIME_INPUT_PIN_KEYS'
              secretRef: 'runtime-input-pin-keys'
            }
            {
              name: 'FAVN_RUNTIME_INPUT_PIN_KEY_VERSION'
              value: string(runtimeInputPinKeyVersion)
            }
            {
              name: 'FAVN_ORCHESTRATOR_API_SERVICE_TOKENS'
              secretRef: 'api-service-tokens'
            }
            {
              name: 'FAVN_WORKSPACE_IDS'
              value: workspaceIds
            }
            {
              name: 'FAVN_RUNNER_POOLS'
              value: runnerPools
            }
            {
              name: 'FAVN_VIEW_PUBLIC_ORIGIN'
              value: viewPublicOrigin
            }
            {
              name: 'FAVN_VIEW_SECRET_KEY_BASE'
              secretRef: 'view-secret-key-base'
            }
            {
              name: 'FAVN_VIEW_TRUSTED_PROXY_CIDRS'
              value: viewTrustedProxyCidrs
            }
            {
              name: 'FAVN_VIEW_FORWARDED_FOR_POLICY'
              value: viewForwardedForPolicy
            }
            {
              name: 'FAVN_VIEW_AUTH_MODE'
              value: enableEntraEasyAuth ? 'azure_container_apps_entra' : 'password'
            }
            {
              name: 'FAVN_VIEW_ENTRA_TENANT_ID'
              value: viewEntraTenantId
            }
            {
              name: 'FAVN_VIEW_ENTRA_WORKSPACE_ID'
              value: viewEntraWorkspaceId
            }
            {
              name: 'RELEASE_NODE'
              value: 'favn-control@favn-control-plane.internal'
            }
            {
              name: 'RELEASE_COOKIE'
              secretRef: 'distribution-cookie'
            }
            {
              name: 'FAVN_DISTRIBUTION_COOKIE'
              secretRef: 'distribution-cookie'
            }
            {
              name: 'FAVN_CONTROL_PLANE_NODE'
              value: 'favn-control@favn-control-plane.internal'
            }
            {
              name: 'FAVN_BEAM_DISTRIBUTION_PORT'
              value: '9100'
            }
            {
              name: 'ERL_EPMD_PORT'
              value: '4369'
            }
            {
              name: 'FAVN_DISTRIBUTION_TLS_OPTIONS_FILE'
              value: '/etc/favn/ssl_dist.config'
            }
            {
              name: 'ERL_AFLAGS'
              value: '-proto_dist inet_tls -ssl_dist_optfile /etc/favn/ssl_dist.config -kernel inet_dist_listen_min 9100 inet_dist_listen_max 9100'
            }
          ]
          resources: {
            cpu: json('0.5')
            memory: '1Gi'
          }
          volumeMounts: [
            {
              volumeName: 'distribution-tls'
              mountPath: '/etc/favn'
            }
          ]
        }
      ]
      volumes: [
        {
          name: 'distribution-tls'
          storageType: 'Secret'
          secrets: [
            {
              secretRef: 'distribution-tls-options'
              path: 'ssl_dist.config'
            }
            {
              secretRef: 'distribution-ca'
              path: 'ca.crt'
            }
            {
              secretRef: 'distribution-certificate'
              path: 'tls.crt'
            }
            {
              secretRef: 'distribution-private-key'
              path: 'tls.key'
            }
            {
              secretRef: 'database-ca'
              path: 'database-ca.crt'
            }
          ]
        }
      ]
      scale: {
        minReplicas: 1
        maxReplicas: 1
      }
    }
  }
}

resource easyAuth 'Microsoft.App/containerApps/authConfigs@2026-01-01' = {
  parent: controlPlane
  name: 'current'
  properties: {
    platform: {
      enabled: enableEntraEasyAuth
    }
    globalValidation: {
      unauthenticatedClientAction: enableEntraEasyAuth ? 'RedirectToLoginPage' : 'AllowAnonymous'
      redirectToProvider: enableEntraEasyAuth ? 'azureactivedirectory' : ''
    }
    httpSettings: {
      requireHttps: true
    }
    identityProviders: enableEntraEasyAuth ? {
      azureActiveDirectory: {
        enabled: true
        registration: {
          clientId: viewEntraClientId
          clientSecretSettingName: 'entra-client-secret'
          openIdIssuer: 'https://login.microsoftonline.com/${viewEntraTenantId}/v2.0'
        }
      }
    } : {}
    login: {
      tokenStore: {
        enabled: false
      }
    }
  }
}
