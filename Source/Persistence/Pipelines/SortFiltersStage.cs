namespace Vinder.Internal.Infrastructure.Persistence.Pipelines;

public static class SortFiltersStage
{
    public static PipelineDefinition<TInput, TOutput> Sort<TInput, TOutput>(
        this PipelineDefinition<TInput, TOutput> pipeline,
        SortFilters? sort)
    {
        if (sort is null || string.IsNullOrWhiteSpace(sort.Field))
            return pipeline;

        var sortDefinition = sort.Direction == Essentials.Filters.SortDirection.Ascending
            ? Builders<TOutput>.Sort.Ascending(sort.Field)
            : Builders<TOutput>.Sort.Descending(sort.Field);

        return pipeline.Sort(sortDefinition);
    }
}