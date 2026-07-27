// One resource instance per exact pool/release. Reference only.
param location string = resourceGroup().location
param environmentId string
param name string
param image string
param runnerPool string
param runnerReleaseId string
param controlPlaneNode string = 'favn-control@favn-control-plane.internal'
param controlPlaneCapacityBaseUrl string
param cpu string
param memory string
param maxExecutions int
param pollingIntervalSeconds int = 5
param replicaTimeoutSeconds int
param runnerMaxUptimeMs int = 3600000
@secure()
param capacityReaderToken string
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

resource runnerJob 'Microsoft.App/jobs@2026-01-01' = {
  name: name
  location: location
  properties: {
    environmentId: environmentId
    configuration: {
      triggerType: 'Event'
      replicaTimeout: replicaTimeoutSeconds
      replicaRetryLimit: 0
      secrets: [
        {
          name: 'capacity-reader-token'
          value: capacityReaderToken
        }
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
      ]
      eventTriggerConfig: {
        parallelism: 1
        replicaCompletionCount: 1
        scale: {
          minExecutions: 0
          maxExecutions: maxExecutions
          pollingInterval: pollingIntervalSeconds
          rules: [
            {
              name: 'favn-runner-demand'
              type: 'metrics-api'
              metadata: {
                url: '${controlPlaneCapacityBaseUrl}/internal/runner-demand/${runnerPool}/${runnerReleaseId}'
                format: 'json'
                valueLocation: 'outstanding'
                targetValue: '1'
                activationTargetValue: '0'
                authMode: 'bearer'
                timeout: '2000'
              }
              auth: [
                {
                  secretRef: 'capacity-reader-token'
                  triggerParameter: 'token'
                }
              ]
            }
          ]
        }
      }
    }
    template: {
      containers: [
        {
          name: 'runner'
          image: image
          env: [
            {
              name: 'FAVN_CONTROL_PLANE_NODE'
              value: controlPlaneNode
            }
            {
              name: 'FAVN_RUNNER_POOL'
              value: runnerPool
            }
            {
              name: 'FAVN_RUNNER_RELEASE_ID'
              value: runnerReleaseId
            }
            {
              name: 'FAVN_RUNNER_LIFECYCLE_MODE'
              value: 'elastic'
            }
            {
              name: 'FAVN_RUNNER_MAX_UPTIME_MS'
              value: string(runnerMaxUptimeMs)
            }
            {
              name: 'FAVN_RUNNER_NODE_HOST_ALIAS'
              value: 'runner.internal'
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
              name: 'FAVN_DISTRIBUTION_TLS_OPTIONS_FILE'
              value: '/etc/favn/ssl_dist.config'
            }
            {
              name: 'ERL_AFLAGS'
              value: '-proto_dist inet_tls -ssl_dist_optfile /etc/favn/ssl_dist.config'
            }
          ]
          resources: {
            cpu: json(cpu)
            memory: memory
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
          ]
        }
      ]
    }
  }
}
