#nullable enable
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Couchbase.Client.Transactions.Components;
using Couchbase.Client.Transactions.DataAccess;
using Couchbase.Client.Transactions.DataModel;
using Couchbase.Client.Transactions.Error;
using Couchbase.Client.Transactions.Internal.Test;
using Couchbase.Client.Transactions.Support;
using Couchbase.Client.Transactions.LogUtil;
using Couchbase.KeyValue;
using Microsoft.Extensions.Logging;

namespace Couchbase.Client.Transactions.Cleanup
{
    internal class Cleaner
    {
        public TestHookMap TestHooks { get; set; } = new();
        public static readonly Task NothingToDo = Task.CompletedTask;

        private readonly ICluster _cluster;
        private readonly string _creatorName;
        private readonly ILogger<Cleaner> _logger;

        public Cleaner(ICluster cluster, ILoggerFactory loggerFactory, [CallerMemberName] string creatorName = nameof(Cleaner))
        {
            _cluster = cluster;
            _creatorName = creatorName;
            _logger = loggerFactory.CreateLogger<Cleaner>();
        }

        public async Task<TransactionCleanupAttempt> ProcessCleanupRequest(CleanupRequest cleanupRequest, bool isRegular = true)
        {
            if (string.IsNullOrEmpty(cleanupRequest.AtrId))
            {
                throw new ArgumentNullException(nameof(cleanupRequest.AtrId));
            }

            _logger.LogDebug("Cleaner.{creator}: Processing cleanup request: {req}", _creatorName, cleanupRequest);
            try
            {
                await Forwards.ForwardCompatibility.Check(null, Forwards.ForwardCompatibility.CleanupEntry, cleanupRequest.ForwardCompatibility).CAF();
                await CleanupDocs(cleanupRequest).CAF();
                await CleanupAtrEntry(cleanupRequest).CAF();
                return new TransactionCleanupAttempt(
                    Success: true,
                    IsRegular: isRegular,
                    AttemptId: cleanupRequest.AttemptId,
                    AtrId: cleanupRequest.AtrId,
                    AtrBucketName: cleanupRequest.AtrCollection.Scope.Bucket.Name,
                    AtrCollectionName: cleanupRequest.AtrCollection.Name,
                    AtrScopeName: cleanupRequest.AtrCollection.Scope.Name,
                    Request: cleanupRequest,
                    FailureReason: null);
            }
            catch (Exception ex)
            {
                _logger.LogDebug("Cleaner.{creator}: Cleanup Failed for {req}!  Reason: {ex}", _creatorName, cleanupRequest, ex);
                // TODO: publish stream of failed cleanups and their cause.
                return new TransactionCleanupAttempt(
                    Success: false,
                    IsRegular: isRegular,
                    AttemptId: cleanupRequest.AttemptId,
                    AtrId: cleanupRequest.AtrId,
                    AtrBucketName: cleanupRequest.AtrCollection.Scope.Bucket.Name,
                    AtrCollectionName: cleanupRequest.AtrCollection.Name,
                    AtrScopeName: cleanupRequest.AtrCollection.Scope.Name,
                    Request: cleanupRequest,
                    FailureReason: ex);
            }
            finally
            {
                _logger.LogDebug("Cleaner.{creator}: Processed cleanup request: {req}", _creatorName, cleanupRequest.AttemptId);
            }
        }

        private Task CleanupDocs(CleanupRequest cleanupRequest) => cleanupRequest.State switch
        {
            AttemptStates.NOTHING_WRITTEN => NothingToDo,
            AttemptStates.PENDING => NothingToDo,
            AttemptStates.ABORTED => CleanupDocsAborted(cleanupRequest),
            AttemptStates.COMMITTED => CleanupDocsCommitted(cleanupRequest),
            AttemptStates.COMPLETED => NothingToDo,
            AttemptStates.ROLLED_BACK => NothingToDo,
            AttemptStates.UNKNOWN => NothingToDo,
            _ => NothingToDo, // ExtUnknownATRStates
        };

        private async Task CleanupAtrEntry(CleanupRequest cleanupRequest)
        {
            using var logScope = _logger.BeginMethodScope();
            _logger.LogInformation("Cleaning up atr entry: {atr}/{attemptId} on {collection}", cleanupRequest.AtrId, cleanupRequest.AttemptId, cleanupRequest.AtrCollection.ToKeySpace());
            try
            {
                TestHooks.Sync(HookPoint.CleanupBeforeAtrRemove, null, cleanupRequest.AtrId);
                var prefix = $"{TransactionFields.AtrFieldAttempts}.{cleanupRequest.AttemptId}";
                var specs = new List<MutateInSpec>();
                if (cleanupRequest.State == AttemptStates.PENDING)
                {
                    specs.Add(MutateInSpec.Insert(
                        $"{prefix}.{TransactionFields.AtrFieldPendingSentinel}",
                        0, isXattr: true));
                }

                specs.Add(MutateInSpec.Remove(prefix, isXattr: true));

                var mutateResult = await cleanupRequest.AtrCollection.MutateInAsync(cleanupRequest.AtrId, specs,
                    opts => opts.Durability(cleanupRequest.GetDurabilityLevel())).CAF();

                if (mutateResult?.MutationToken.SequenceNumber != 0)
                {
                    _logger.LogInformation("Attempt {attemptId}: ATR {atr} cleaned up.", cleanupRequest.AttemptId, cleanupRequest.AtrId);
                }
                else
                {
                    _logger.LogWarning("Attempt {attemptId}: ATR {atr} cleanup failed on MutateIn.", cleanupRequest.AttemptId, cleanupRequest.AtrId);
                }
            }
            catch (Exception ex)
            {
                var ec = ex.Classify();
                if (ec == ErrorClass.FailPathNotFound)
                {
                    _logger.LogDebug("PathNotFound by the time cleanup attempted: {atr}/{attemptId}", cleanupRequest.AtrId, cleanupRequest.AttemptId);
                    return;
                }

                _logger.LogWarning("Failed to cleanup ATR {atr}/{attemptId}: {reason}", cleanupRequest.AtrId, cleanupRequest.AttemptId, ex);

                throw;
            }
        }

        private async Task CleanupDocsAborted(CleanupRequest cleanupRequest)
        {
            using var logScope = _logger.BeginMethodScope();
            var sw = Stopwatch.StartNew();
            foreach (var dr in cleanupRequest.InsertedIds)
            {
                await CleanupDoc(dr, requireCrc32ToMatchStaging: false, attemptId: cleanupRequest.AttemptId,
                    perDoc: async (op) =>
                    {
                        await TestHooks.Async(HookPoint.CleanupBeforeRemoveDoc, null, dr.Id).CAF();
                        var collection = await dr.GetCollection(_cluster).CAF();
                        var finalDoc = op.StagedContent!.ContentAs<object>();
                        if (op.IsDeleted)
                        {
                            await collection.MutateInAsync(dr.Id, specs =>
                                    specs.Remove(TransactionFields.TransactionInterfacePrefixOnly, isXattr: true),
                                opts => opts.Cas(op.Cas)
                                    .Durability(cleanupRequest.GetDurabilityLevel())
                                    .AccessDeleted(true)).CAF();
                        }
                        else
                        {
                            await collection.RemoveAsync(dr.Id, opts => opts.Cas(op.Cas)
                                .Durability(cleanupRequest.GetDurabilityLevel())).CAF();
                        }
                    }).CAF();
            }

            var replacedOrRemoved = cleanupRequest.ReplacedIds.Concat(cleanupRequest.RemovedIds);
            foreach (var dr in replacedOrRemoved)
            {
                await CleanupDoc(dr, requireCrc32ToMatchStaging: false, attemptId: cleanupRequest.AttemptId,
                    perDoc: async (op) =>
                    {
                        await TestHooks.Async(HookPoint.CleanupBeforeRemoveDocLinks, null, dr.Id).CAF();
                        var collection = await dr.GetCollection(_cluster).CAF();
                        await collection.MutateInAsync(dr.Id, specs =>
                                specs.Remove(TransactionFields.TransactionInterfacePrefixOnly, isXattr: true),
                            opts => opts.Cas(op.Cas)
                                .AccessDeleted(true)).CAF();
                    }).CAF();
            }

            sw.Stop();
            _logger.LogWarning("{method} took {elapsed}ms", nameof(CleanupDocsAborted), sw.Elapsed.TotalMilliseconds);
        }

        private async Task CleanupDocsCommitted(CleanupRequest cleanupRequest)
        {
            using var logScope = _logger.BeginMethodScope();
            var insertedOrReplaced = cleanupRequest.InsertedIds.Concat(cleanupRequest.ReplacedIds);
            var durabilityLevel = cleanupRequest.GetDurabilityLevel();
            foreach (var dr in insertedOrReplaced)
            {
                await CleanupDoc(dr, requireCrc32ToMatchStaging: true, attemptId: cleanupRequest.AttemptId,
                    perDoc: async (op) =>
                    {
                        // TODO: This has significant overlap with UnstageInsertOrReplace.
                        TestHooks.Sync(HookPoint.CleanupBeforeCommitDoc, null, dr.Id);
                        var collection = await dr.GetCollection(_cluster).CAF();
                        var finalDoc = op.StagedContent!.ContentAs<object>();
                        if (op.IsDeleted)
                        {
                            await collection.InsertAsync(dr.Id, finalDoc, opts => opts.Durability(durabilityLevel)).CAF();
                        }
                        else
                        {
                            await collection.MutateInAsync(dr.Id, specs =>
                                    specs.Remove(TransactionFields.TransactionInterfacePrefixOnly, isXattr: true)
                                        .SetDoc(finalDoc)
                                , opts => opts.Cas(op.Cas)
                                    .Durability(durabilityLevel)).CAF();
                        }
                    }).CAF();
            }

            foreach (var dr in cleanupRequest.RemovedIds)
            {
                await CleanupDoc(dr, requireCrc32ToMatchStaging: true, attemptId: cleanupRequest.AttemptId,
                    perDoc: async (op) =>
                    {
                        TestHooks.Sync(HookPoint.CleanupBeforeRemoveDocStagedForRemoval, null, dr.Id);
                        var collection = await dr.GetCollection(_cluster).CAF();

                        await collection.RemoveAsync(dr.Id, opts => opts.Cas(op.Cas)
                            .Durability(durabilityLevel)).CAF();
                    }).CAF();
            }
        }

        public async Task CleanupDoc(DocRecord dr, bool requireCrc32ToMatchStaging, Func<DocumentLookupResult, Task> perDoc, string attemptId)
        {
            TestHooks.Sync(HookPoint.CleanupBeforeDocGet, null, dr.Id);
            var collection = await dr.GetCollection(_cluster).CAF();
            var docLookupResult = await DocumentRepository.LookupDocumentStaticAsync(collection, dr.Id, fullDocument: false).CAF();

            if (docLookupResult.TransactionXattrs == null)
            {
                return;
            }

            if (docLookupResult.TransactionXattrs.Id?.AttemptId != attemptId)
            {
                return;
            }

            if (requireCrc32ToMatchStaging && !string.IsNullOrEmpty(docLookupResult.DocumentMetadata?.Crc32c))
            {
                if (docLookupResult.DocumentMetadata?.Crc32c != docLookupResult.TransactionXattrs.Operation?.Crc32)
                {
                    // "the world has moved on", continue as success
                    return;
                }
            }

            // If we reach here, the document is unchanged from staging, and it's safe to proceed
            await perDoc(docLookupResult).ConfigureAwait(false);
        }
    }
}


/* ************************************************************
 *
 *    @author Couchbase <info@couchbase.com>
 *    @copyright 2024 Couchbase, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 * ************************************************************/







