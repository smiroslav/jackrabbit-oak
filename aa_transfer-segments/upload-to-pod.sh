POD=$(kubectl get pods -l app=test-diskcache-aem-publish | grep Running | cut -d " " -f 1)
kubectl cp aem ${POD}:/mnt/sandbox/cache/aem