#pragma warning disable CA2200

namespace Vinder.Internal.Infrastructure.Persistence.Repositories;

public abstract class BaseCollection<TEntity>(IMongoDatabase database, string collection) :
    IBaseCollection<TEntity> where TEntity : Entity
{
    protected readonly IMongoCollection<TEntity> _collection = database.GetCollection<TEntity>(collection);

    public virtual async Task<bool> DeleteAsync(TEntity entity, DeleteBehavior behavior = DeleteBehavior.Soft, CancellationToken cancellation = default)
    {
        var filter = Builders<TEntity>.Filter.Eq(entity => entity.Id, entity.Id);

        entity.MarkAsDeleted();
        entity.MarkAsUpdated();

        return behavior switch
        {
            DeleteBehavior.Soft => (await _collection.ReplaceOneAsync(filter, entity, cancellationToken: cancellation)).ModifiedCount > 0,
            DeleteBehavior.Hard => (await _collection.DeleteOneAsync(filter, cancellationToken: cancellation)).DeletedCount > 0,

            _ => false
        };
    }

    public virtual async Task<TEntity> InsertAsync(TEntity entity, WriteBehavior behavior = WriteBehavior.FailIfExists, CancellationToken cancellation = default)
    {
        entity.Id = Identifier.Generate<TEntity>();
        entity.CreatedAt = DateTime.UtcNow;

        try
        {
            await _collection.InsertOneAsync(entity, cancellationToken: cancellation);
            return entity;
        }
        catch (MongoWriteException exception) when (exception.WriteError.Category == ServerErrorCategory.DuplicateKey)
        {
            var filter = Builders<TEntity>.Filter.Eq(entity => entity.Id, entity.Id);

            return behavior switch
            {
                WriteBehavior.IgnoreIfExists => entity,

                // infrastructure boundary: exception is intentionally rethrown inside a switch expression.
                // preserving the original stack trace is not required for this propagation.
                WriteBehavior.FailIfExists => throw exception,
                WriteBehavior.Overwrite => await _collection
                    .ReplaceOneAsync(filter, entity, cancellationToken: cancellation)
                    .ContinueWith(_ => entity, cancellation),
            };
        }
    }

    public virtual async Task InsertManyAsync(IEnumerable<TEntity> entities, WriteBehavior behavior = WriteBehavior.FailIfExists, CancellationToken cancellation = default)
    {
        Parallel.ForEach(entities, entity =>
        {
            entity.Id = Identifier.Generate<TEntity>();
            entity.CreatedAt = DateTime.UtcNow;
        });

        try
        {
            await _collection.InsertManyAsync(entities, cancellationToken: cancellation);
        }
        catch (MongoBulkWriteException exception) when (exception.WriteErrors.Any(error => error.Category == ServerErrorCategory.DuplicateKey))
        {
            await (behavior switch
            {
                WriteBehavior.IgnoreIfExists => Task.CompletedTask,

                // infrastructure boundary: exception is intentionally rethrown inside a switch expression.
                // preserving the original stack trace is not required for this propagation.
                WriteBehavior.FailIfExists => throw exception,
                WriteBehavior.Overwrite => Task.WhenAll(
                    entities.Select(entity => _collection.ReplaceOneAsync(Builders<TEntity>.Filter.Eq(parameter => parameter.Id, entity.Id), entity, cancellationToken: cancellation))),
            });
        }
    }

    public virtual async Task<TEntity> UpdateAsync(TEntity entity, CancellationToken cancellation = default)
    {
        entity.MarkAsUpdated();

        var filter = Builders<TEntity>.Filter.Eq(entity => entity.Id, entity.Id);

        await _collection.ReplaceOneAsync(filter, entity, cancellationToken: cancellation);
        return entity;
    }
}