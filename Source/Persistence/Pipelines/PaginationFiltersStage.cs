namespace Vinder.Internal.Infrastructure.Persistence.Pipelines;

public static class PageFiltersStage
{
    public static PipelineDefinition<TInput, TOutput> Paginate<TInput, TOutput>(
        this PipelineDefinition<TInput, TOutput> pipeline,
        PaginationFilters? filters)
    {
        if (filters is null)
            return pipeline;

        return pipeline
            .Skip(filters.Skip)
            .Limit(filters.Take);
    }
}