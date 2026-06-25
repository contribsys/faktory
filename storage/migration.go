package storage

import (
	"context"
)

var (
	Migrations = []func(context.Context, Store) string{
		// a list of migration functions to apply in order,
		// each one will increment the data version
		prefixQueues,
	}
)

func (store *redisStore) DataVersion(ctx context.Context) (int64, error) {
	return store.rclient.IncrBy(ctx, "v", 0).Result()
}

func (store *redisStore) ApplyMigrations(ctx context.Context) (int64, error) {
	curVer, err := store.DataVersion(ctx)
	if err != nil {
		return 0, err
	}

	idx := curVer
	for ; idx < int64(len(Migrations)); idx++ {
		_, err := store.applyMigration(ctx, idx)
		if err != nil {
			return 0, err
		}
	}

	return store.DataVersion(ctx)
}

func (store *redisStore) applyMigration(ctx context.Context, curVer int64) (int64, error) {
	fn := Migrations[curVer]
	script := fn(ctx, store)
	return store.rclient.Eval(ctx, script, []string{}, []any{}).Int64()
}

func prefixQueues(ctx context.Context, store Store) string {
	return `
		local list = redis.call('smembers', "queues")
		local prefix = 'q:'
	
		for i, v in ipairs(list) do
			if redis.call('exists', v) == 1 then
				redis.call('rename', v, prefix .. v)
			end
		end

		return redis.call('incrby', 'v', 1)
	`
}
