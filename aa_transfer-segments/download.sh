SAS_TOKEN='st=2020-02-28T10%3A08%3A27Z&se=2020-02-29T10%3A08%3A27Z&sp=racwdl&sv=2018-03-28&sr=c&sig=n2oBHUCD21OMUB0z%2FZkfLfwozB%2FZ0nZaM4nH%2FNRUolM%3D'

REV=000001
echo using revision $REV
azcopy copy --recursive --exclude-pattern="journal.log*;gc.log;repo.lock"  \
  "https://aemk8s.blob.core.windows.net/aem-sgmt-60391c9c59df7c26a58d42207b32ad11eb93061a-${REV}/aem/*?${SAS_TOKEN}" \
  aem/
