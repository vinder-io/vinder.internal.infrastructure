namespace Vinder.Internal.Infrastructure.Persistence;

public abstract class BaseRepository<TEntity>(IMongoDatabase database, string collection) :
    IBaseRepository<TEntity> where TEntity : Entity
{
    protected readonly IMongoCollection<TEntity> _collection = database.GetCollection<TEntity>(collection);

    public virtual async Task<bool> DeleteAsync(TEntity entity, CancellationToken cancellation = default)
    {
        entity.MarkAsDeleted();
        entity.MarkAsUpdated();

        var filter = Builders<TEntity>.Filter.Eq(entity => entity.Id, entity.Id);
        var result = await _collection.ReplaceOneAsync(filter, entity, cancellationToken: cancellation);

        return result.IsAcknowledged &&
               result.ModifiedCount > 0;
    }

    public virtual async Task<TEntity> InsertAsync(TEntity entity, CancellationToken cancellation = default)
    {
        entity.Id = Identifier.Generate<TEntity>();

        await _collection.InsertOneAsync(entity, cancellationToken: cancellation);

        return entity;
    }

    public virtual async Task InsertManyAsync(IEnumerable<TEntity> entities, CancellationToken cancellation = default)
    {
        Parallel.ForEach(entities, entity =>
        {
            entity.Id = Identifier.Generate<TEntity>();
        });

        await _collection.InsertManyAsync(entities, cancellationToken: cancellation);
    }

    public virtual async Task<TEntity> UpdateAsync(TEntity entity, CancellationToken cancellation = default)
    {
        entity.MarkAsUpdated();

        var filter = Builders<TEntity>.Filter.Eq(entity => entity.Id, entity.Id);

        await _collection.ReplaceOneAsync(filter, entity, cancellationToken: cancellation);
        return entity;
    }
}