namespace Vinder.Internal.Infrastructure.Persistence.Repositories;

public sealed class ActivityCollection(IMongoDatabase database) :
    BaseCollection<Activity>(database, Collections.Activities),
    IActivityCollection
{
    public async Task<IReadOnlyCollection<Activity>> GetActivitiesAsync(
        ActivityFilters filters, CancellationToken cancellation = default)
    {
        var pipeline = PipelineDefinitionBuilder
            .For<Activity>()
            .As<Activity, Activity, BsonDocument>()
            .FilterActivities(filters)
            .Paginate(filters.Pagination)
            .Sort(filters.Sort);

        var options = new AggregateOptions { AllowDiskUse = true };
        var aggregation = await _collection.AggregateAsync(pipeline, options, cancellation);

        var bsonDocuments = await aggregation.ToListAsync(cancellation);
        var activities = bsonDocuments
            .Select(bson => BsonSerializer.Deserialize<Activity>(bson))
            .ToList();

        return activities;
    }

    public async Task<long> CountAsync(ActivityFilters filters, CancellationToken cancellation = default)
    {
        var pipeline = PipelineDefinitionBuilder
            .For<Activity>()
            .As<Activity, Activity, BsonDocument>()
            .FilterActivities(filters)
            .Count();

        var aggregation = await _collection.AggregateAsync(pipeline, cancellationToken: cancellation);
        var result = await aggregation.FirstOrDefaultAsync(cancellation);

        return result?.Count ?? 0;
    }
}