#pragma warning disable CA2200

namespace Vinder.Internal.Infrastructure.Persistence;

public abstract class AggregateCollection<TAggregate>(IMongoDatabase database, string collection) :
    IAggregateCollection<TAggregate> where TAggregate : Aggregate
{
    protected readonly IMongoCollection<TAggregate> _collection = database.GetCollection<TAggregate>(collection);

    public virtual async Task<bool> DeleteAsync(TAggregate aggregate, DeletionBehavior behavior = DeletionBehavior.Soft, CancellationToken cancellation = default)
    {
        var filter = Builders<TAggregate>.Filter.Eq(aggregate => aggregate.Id, aggregate.Id);

        aggregate.MarkAsDeleted();
        aggregate.MarkAsUpdated();

        return behavior switch
        {
            DeletionBehavior.Soft => (await _collection.ReplaceOneAsync(filter, aggregate, cancellationToken: cancellation)).ModifiedCount > 0,
            DeletionBehavior.Hard => (await _collection.DeleteOneAsync(filter, cancellationToken: cancellation)).DeletedCount > 0,

            _ => false
        };
    }

    public virtual async Task<TAggregate> InsertAsync(TAggregate aggregate, InsertionBehavior behavior = InsertionBehavior.FailIfExists, CancellationToken cancellation = default)
    {
        aggregate.Id = Identifier.Generate<TAggregate>();
        aggregate.CreatedAt = DateTime.UtcNow;

        try
        {
            await _collection.InsertOneAsync(aggregate, cancellationToken: cancellation);
            return aggregate;
        }
        catch (MongoWriteException exception) when (exception.WriteError.Category == ServerErrorCategory.DuplicateKey)
        {
            var filter = Builders<TAggregate>.Filter.Eq(aggregate => aggregate.Id, aggregate.Id);

            return behavior switch
            {
                InsertionBehavior.IgnoreIfExists => aggregate,

                // infrastructure boundary: exception is intentionally rethrown inside a switch expression.
                // preserving the original stack trace is not required for this propagation.
                InsertionBehavior.FailIfExists => throw exception,
                InsertionBehavior.Overwrite => await _collection
                    .ReplaceOneAsync(filter, aggregate, cancellationToken: cancellation)
                    .ContinueWith(_ => aggregate, cancellation),
            };
        }
    }

    public virtual async Task InsertManyAsync(IEnumerable<TAggregate> aggregates, InsertionBehavior behavior = InsertionBehavior.FailIfExists, CancellationToken cancellation = default)
    {
        Parallel.ForEach(aggregates, aggregate =>
        {
            aggregate.Id = Identifier.Generate<TAggregate>();
            aggregate.CreatedAt = DateTime.UtcNow;
        });

        try
        {
            await _collection.InsertManyAsync(aggregates, cancellationToken: cancellation);
        }
        catch (MongoBulkWriteException exception) when (exception.WriteErrors.Any(error => error.Category == ServerErrorCategory.DuplicateKey))
        {
            await (behavior switch
            {
                InsertionBehavior.IgnoreIfExists => Task.CompletedTask,

                // infrastructure boundary: exception is intentionally rethrown inside a switch expression.
                // preserving the original stack trace is not required for this propagation.
                InsertionBehavior.FailIfExists => throw exception,
                InsertionBehavior.Overwrite => Task.WhenAll(
                    aggregates.Select(aggregate => _collection.ReplaceOneAsync(Builders<TAggregate>.Filter.Eq(parameter => parameter.Id, aggregate.Id), aggregate, cancellationToken: cancellation))),
            });
        }
    }

    public virtual async Task<TAggregate> UpdateAsync(TAggregate aggregate, CancellationToken cancellation = default)
    {
        aggregate.MarkAsUpdated();

        var filter = Builders<TAggregate>.Filter.Eq(aggregate => aggregate.Id, aggregate.Id);

        await _collection.ReplaceOneAsync(filter, aggregate, cancellationToken: cancellation);
        return aggregate;
    }
}