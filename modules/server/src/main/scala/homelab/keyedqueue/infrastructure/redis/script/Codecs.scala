package homelab.keyedqueue.infrastructure.redis.script


import homelab.keyedqueue.domain.model.Message
import homelab.keyedqueue.domain.model.Settlement.Verdict
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.codecs.storage.StoredMessage
import zio.{ Chunk, Duration }
import homelab.keyedqueue.infrastructure.redis.RedisFailure
import homelab.keyedqueue.infrastructure.redis.script.lock.*
import homelab.keyedqueue.infrastructure.redis.script.queue.*
import homelab.keyedqueue.infrastructure.redis.script.LuaScript.Input.Encoder
import homelab.keyedqueue.infrastructure.redis.script.LuaScript.Output.Decoder


/**
 * How each script's input is written and its reply read — one place, so a script is a digest, a shape and
 * nothing else.
 *
 * The two directions are kept apart below rather than paired per script: what a script is *sent* and what
 * it *answers* have no reason to be read together, and the reply codecs are the ones anyone debugging a
 * decoding failure comes looking for.
 */
object Codecs {
  // -- Encoder

  /**
   * Where a claim comes from and everything it writes, then what the script needs to build the key's own
   * structures and bound the claim.
   *
   * The key's own structures — `msgs`, `payloads`, `owned` — are absent from the keys: their names depend
   * on the key the script chooses, and it builds them from the prefix. Legal because all of them share the
   * partition's hash tag, and so its slot.
   */
  given claimInput: LuaScript.Input.Encoder[ClaimScript.Input] = claim =>
    LuaScript.Input(
      key = Array[String](
        claim.keys.ready,
        claim.keys.claimed,
        claim.keys.fence,
        claim.keys.attempts,
      ),
      args = Array(
        Encoder.utf8(claim.keys.prefix),
        Encoder.millis(claim.leaseTtl),
        Encoder.number(claim.maxBatch),
      ),
    )

  /**
   * The five structures an append touches, then the key to append under and the message as it will be
   * stored.
   */
  given enqueueInput: LuaScript.Input.Encoder[EnqueueScript.Input] = enqueue =>
    LuaScript.Input(
      key = Array[String](
        enqueue.keys.ready,
        enqueue.keys.claimed,
        enqueue.keys.delayed,
        enqueue.keys.msgs(enqueue.message.key),
        enqueue.keys.payloads(enqueue.message.key),
        enqueue.keys.wake,
        enqueue.keys.sequence,
      ),
      args = Array(
        Encoder.utf8(enqueue.message.key),
        Encoder.utf8(enqueue.message.messageId),
        StoredMessage.toBytes(enqueue.message).toArray,
        Encoder.utf8(enqueue.keys.queue),
      ),
    )

  /**
   * Everything a settle may touch, then the claim that authorises it and what became of which message.
   *
   * The outcomes are flattened pairwise — id, then verdict — because a Lua script reads ARGV positionally
   * and has no other way to be handed a list of pairs.
   */
  given settleInput: LuaScript.Input.Encoder[SettleScript.Input] = settle =>
    val claim = settle.settlement.claimed
    LuaScript.Input(
      key = Array[String](
        settle.keys.ready,
        settle.keys.claimed,
        settle.keys.fence,
        settle.keys.msgs(claim.key),
        settle.keys.payloads(claim.key),
        settle.keys.owned(claim.key),
        settle.keys.attempts,
        settle.keys.delayed,
        settle.keys.wake,
        settle.keys.sequence,
      ),
      args = (Chunk(
        Encoder.utf8(claim.key),
        Encoder.number(claim.token),
        Encoder.millis(settle.settlement.retryAfter.getOrElse(Duration.Zero)),
        Encoder.utf8(claim.queue),
      ) ++ settle.settlement.outcomes.toChunk.flatMap(outcome =>
        Chunk(
          Encoder.utf8(outcome.messageId),
          Encoder.utf8(if outcome.verdict == Verdict.Done then "ack" else "nack"),
        )
      )).toArray,
    )

  /**
   * The leases to push forward and the fences that say whether a claim is still the caller's, then the
   * lease length and the held claims flattened into key-and-token pairs.
   *
   * The token travels with each key because a beat must not renew a claim the caller no longer owns: the
   * script checks it against the fence and reports the key as lost instead.
   */
  given renewInput: LuaScript.Input.Encoder[RenewScript.Input] = renew => {
    val pairs = renew.held.flatMap: claim =>
      Chunk(
        Encoder.utf8(claim.key),
        Encoder.number(claim.token),
      )
    LuaScript.Input(
      key = Array[String](renew.keys.claimed, renew.keys.fence),
      args = (Chunk(Encoder.millis(renew.leaseTtl)) ++ pairs).toArray,
    )
  }

  /** What the two sweeps read and repair, then the batch bound and the names the script rebuilds keys from. */
  given sweepInput: LuaScript.Input.Encoder[SweepScript.Input] = sweep =>
    LuaScript.Input(
      key = Array[String](
        sweep.keys.claimed,
        sweep.keys.ready,
        sweep.keys.fence,
        sweep.keys.delayed,
        sweep.keys.wake,
        sweep.keys.sequence,
      ),
      args = Array(
        Encoder.number(sweep.limit),
        Encoder.utf8(sweep.keys.prefix),
        Encoder.utf8(sweep.keys.queue),
      ),
    )

  /** The lock's name, the lease length, then how long the ticket lives. */
  given lockAcquireInput: LuaScript.Input.Encoder[AcquireScript.Input] = acquire =>
    LuaScript.Input(
      key = acquire.keys.granting(acquire.name),
      args = Array(
        Encoder.utf8(acquire.name),
        Encoder.millis(acquire.ttl),
        Encoder.millis(acquire.patience),
      ),
    )

  /** The lock's name, the ticket asking, then the lease length a grant would run for. */
  given lockGrantInput: LuaScript.Input.Encoder[GrantScript.Input] = grant =>
    LuaScript.Input(
      key = grant.keys.granting(grant.name),
      args = Array(
        Encoder.utf8(grant.name),
        Encoder.number(grant.ticket),
        Encoder.millis(grant.ttl),
      ),
    )

  /** The lock's name, then the lease length. */
  given lockTryInput: LuaScript.Input.Encoder[TryScript.Input] = attempt =>
    LuaScript.Input(
      key = attempt.keys.granting(attempt.name),
      args = Array(Encoder.utf8(attempt.name), Encoder.millis(attempt.ttl)),
    )

  /** The lock's name, the token that authorises extending it, then how much longer to grant. */
  given lockRefreshInput: LuaScript.Input.Encoder[RefreshScript.Input] = refresh =>
    LuaScript.Input(
      key = refresh.keys.refresh,
      args = Array(
        Encoder.utf8(refresh.name),
        Encoder.number(refresh.token),
        Encoder.millis(refresh.ttl),
      ),
    )

  /** The lock's name, then the token that authorises releasing it. */
  given lockReleaseInput: LuaScript.Input.Encoder[ReleaseScript.Input] = release =>
    LuaScript.Input(
      key = release.keys.release,
      args = Array(Encoder.utf8(release.name), Encoder.number(release.token)),
    )

  /** The lock's name, then the ticket being withdrawn. */
  given lockAbandonInput: LuaScript.Input.Encoder[AbandonScript.Input] = abandon =>
    LuaScript.Input(
      key = abandon.keys.ticket(abandon.name),
      args = Array(Encoder.utf8(abandon.name), Encoder.number(abandon.ticket)),
    )

  /**
   * The grace, the batch bound, then the prefix the script builds waiter-list keys from.
   *
   * The prefix is an argument and not a key because the names it completes depend on what the trim finds.
   */
  given lockTrimInput: LuaScript.Input.Encoder[TrimScript.Input] = trim =>
    val keys = trim.keys
    LuaScript.Input(
      key = keys.trim,
      args = Array(
        Encoder.millis(trim.grace),
        Encoder.number(trim.limit),
        Encoder.utf8(keys.waitersPrefix),
      ),
    )

  // -- Decoder

  /** One message as it was stored — bulk-string bytes, read back through this adapter's storage codec. */
  given message: LuaScript.Output.Decoder[Message] = Decoder.bytes.emap: bytes =>
    StoredMessage.fromBytes(bytes).left.map(f => RedisFailure.DecodingError(f.message))

  /**
   * The shape the claim script promises: `{key, token, deadline, backlog, ids, messages, attempts}` when
   * it granted a claim, and absence when nothing was claimable.
   *
   * The last three are parallel arrays — one entry per message, in producer order — so their alignment is
   * the contract. The stored bytes are read back here, so an unreadable message fails the claim.
   *
   * The queue is not in it: the script is told a prefix and answers with a key, so naming the queue — and
   * so building a `Grant` — belongs to the caller that chose the namespace.
   */
  given claimOutput: LuaScript.Output.Decoder[ClaimScript.Output] =
    Decoder
      .sized(7) {
        for
          key      <- Decoder.text.at(0).map(MessageKey(_))
          token    <- Decoder.long.at(1).map(Token(_))
          deadline <- Decoder.instant.at(2)
          backlog  <- Decoder.int.at(3)
          ids      <- Decoder.text.many.at(4)
          messages <- Decoder[Message].many.at(5)
          attempts <- Decoder.int.many.at(6)
          batch    <- Decoder.nonEmpty(ClaimScript.Output.owned(ids, messages, attempts))
        yield (key, token, deadline, backlog, batch)
      }
      .orNone

  /**
   * An integer reply, as every script that answers a count spells one.
   *
   * Shared rather than written per script: a depth, a length and a counter are the same reply, and which
   * script produced it is not something a decoder can or should know.
   */
  given count: LuaScript.Output.Decoder[Long] = Decoder.long

  /**
   * An integer reply that means yes or no — `1` when the script applied, `0` when it refused.
   *
   * The convention every settle, release and refresh shares, so it is stated once.
   */
  given applied: LuaScript.Output.Decoder[Boolean] = Decoder.long.map(_ == 1L)

  /** The deadline every renewed claim now carries, and the keys the script could not renew. */
  given renewOutput: LuaScript.Output.Decoder[RenewScript.Output] =
    Decoder.sized(2) {
      for
        renewedUntil <- Decoder.instant.at(0)
        lost         <- Decoder.text.many.at(1).map(_.map(MessageKey(_)))
      yield (renewedUntil, lost)
    }

  /** What one sweep repaired: the keys it reclaimed from lapsed claims, and the ones whose backoff elapsed. */
  given sweepOutput: LuaScript.Output.Decoder[SweepScript.Output] =
    Decoder.sized(2) {
      for
        reclaimed <- Decoder.text.many.at(0).map(_.map(MessageKey(_)))
        released  <- Decoder.text.many.at(1).map(_.map(MessageKey(_)))
      yield QueueStore.Swept(reclaimed, released)
    }

  /** `{1, token, leaseUntil}` when the lock was taken, `{0, ticketId, recheckMillis}` when it was queued for. */
  given lockAcquireOutput: LuaScript.Output.Decoder[AcquireScript.Output] =
    Decoder.sized(3) {
      Decoder.long.at(0).flatMap {
        case 1     =>
          for
            token <- Decoder.long.at(1).map(Token(_))
            until <- Decoder.instant.at(2)
          yield AcquireScript.Entered.Granted(token, until)
        case 0     =>
          for
            ticket  <- Decoder.long.at(1)
            recheck <- Decoder.duration.at(2)
          yield AcquireScript.Entered.Queued(ticket, recheck)
        case other => Decoder.fail(s"lock.acquire answered with status $other")
      }
    }

  /** `{1, token, leaseUntil}` granted, `{0, recheckMillis}` not yet, `{2, 0}` when the ticket is gone. */
  given lockGrantOutput: LuaScript.Output.Decoder[GrantScript.Output] =
    Decoder.long.at(0).flatMap {
      case 1     =>
        for
          token <- Decoder.long.at(1).map(Token(_))
          until <- Decoder.instant.at(2)
        yield GrantScript.Asked.Granted(token, until)
      case 0     => Decoder.duration.at(1).map(GrantScript.Asked.Wait(_))
      case 2     => Decoder.constant(GrantScript.Asked.Gone)
      case other => Decoder.fail(s"lock.grant answered with status $other")
    }

  /** `{token, leaseUntil}`, or nil when the lock is held or queued for. */
  given lockTryOutput: LuaScript.Output.Decoder[TryScript.Output] =
    Decoder
      .sized(2) {
        for
          token <- Decoder.long.at(0).map(Token(_))
          until <- Decoder.instant.at(1)
        yield (token, until)
      }
      .orNone

  /** `{leaseUntil, ok}` — the new deadline, and whether the hold survived to take it. */
  given lockRefreshOutput: LuaScript.Output.Decoder[RefreshScript.Output] =
    Decoder.sized(2) {
      for
        until   <- Decoder.instant.at(0)
        renewed <- Decoder.long.at(1).map(_ == 1L)
      yield (until, renewed)
    }

  /** The names a trim removed, oldest lease first. */
  given lockTrimOutput: LuaScript.Output.Decoder[TrimScript.Output] =
    Decoder.text.many.map(_.map(LockName(_)))
}
