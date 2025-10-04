namespace Vinder.Internal.Infrastructure.Persistence.Pipelines;

public static class ActivityFiltersStage
{
    public static PipelineDefinition<Activity, BsonDocument> FilterActivities(
        this PipelineDefinition<Activity, BsonDocument> pipeline,
        ActivityFilters filters)
    {
        var definitions = new List<FilterDefinition<BsonDocument>>
        {
            FilterDefinitions.MatchIfNotEmpty(Documents.Activity.Identifier, filters.Id),
            FilterDefinitions.MatchIfNotEmpty(Documents.Activity.Action, filters.Action),
            FilterDefinitions.MatchIfNotEmpty(Documents.Activity.TenantId, filters.TenantId),
            FilterDefinitions.MatchIfNotEmpty(Documents.Activity.Resource, filters.ResourceId),
            FilterDefinitions.MatchBool(Documents.Activity.UserId, filters.IsDeleted)
        };

        return pipeline.Match(Builders<BsonDocument>.Filter.And(definitions));
    }
}