class SentenceGrouper:
    def __init__(self, bucket_lengths):
        self.bucket_lengths = bucket_lengths
        
    def get_bucket_id(self, length):
        return next(
            (
                i
                for i, bucket_len in enumerate(self.bucket_lengths)
                if length <= bucket_len
            ),
            len(self.bucket_lengths),
        )
        
    def slice(self, dataset, batch_size = 32):
        buckets = [[] for _ in self.bucket_lengths]
        buckets.append([])

        for entry in dataset:
            length = len(entry['words'])
            bucket_id = self.get_bucket_id(length)
            buckets[bucket_id].append(entry)

            if len(buckets[bucket_id]) >= batch_size:
                yield buckets[bucket_id][:]
                buckets[bucket_id] = []

        for bucket in buckets:
            if len(bucket) > 0:
                yield bucket                
