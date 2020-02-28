POD=$(kubectl get pods -l app=test-diskcache-aem-publish | grep Running | cut -d " " -f 1)
kubectl exec -it  ${POD} mkdir -- -p /mnt/sandbox/cache/

kubectl cp aem ${POD}:/mnt/sandbox/cache/aem