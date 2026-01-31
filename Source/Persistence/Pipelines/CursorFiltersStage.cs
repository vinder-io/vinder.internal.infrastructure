namespace Vinder.Internal.Infrastructure.Persistence.Pipelines;

public static class CursorFiltersStage
{
    public static PipelineDefinition<TInput, BsonDocument> Cursor<TInput>(
        this PipelineDefinition<TInput, BsonDocument> pipeline, CursorFilters? filters)
        where TInput : Aggregate
    {
        if (filters is null)
            return pipeline;

        if (!string.IsNullOrWhiteSpace(filters.Cursor))
        {
            var createdAt = CursorEncoder.Decode(filters.Cursor);
            var filter = Builders<BsonDocument>.Filter.Lt(nameof(Aggregate.CreatedAt), createdAt);

            pipeline = pipeline.Match(filter);
        }

        return pipeline.Limit(filters.Limit);
    }
}