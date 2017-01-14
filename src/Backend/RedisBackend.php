<?php
namespace B3N\TYPO3\Cache\Backend;

use Predis\Client;
use TYPO3\CMS\Core\Cache\Backend\AbstractBackend;
use TYPO3\CMS\Core\Cache\Backend\TaggableBackendInterface;
use TYPO3\CMS\Core\Utility\StringUtility;

class RedisBackend extends AbstractBackend implements TaggableBackendInterface
{

    /**
     * Key prefix for identifier->data entries
     *
     * @var string
     */
    const IDENTIFIER_DATA_PREFIX = 'identData:';

    /**
     * Key prefix for identifier->tags sets
     *
     * @var string
     */
    const IDENTIFIER_TAGS_PREFIX = 'identTags:';

    /**
     * Key prefix for tag->identifiers sets
     *
     * @var string
     */
    const TAG_IDENTIFIERS_PREFIX = 'tagIdents:';

    /**
     * @var Client
     */
    private $redisClient;

    /**
     * Faked unlimited lifetime = 31536000 (1 Year).
     * In redis an entry does not have a lifetime by default (it's not "volatile").
     * Entries can be made volatile either with EXPIRE after it has been SET,
     * or with SETEX, which is a combined SET and EXPIRE command.
     * But an entry can not be made "unvolatile" again. To set a volatile entry to
     * not volatile again, it must be DELeted and SET without a following EXPIRE.
     * To save these additional calls on every set(),
     * we just make every entry volatile and treat a high number as "unlimited"
     *
     * @see http://code.google.com/p/redis/wiki/ExpireCommand
     * @var int Faked unlimited lifetime
     */
    const FAKED_UNLIMITED_LIFETIME = 31536000;

    /**
     * Indicates whether data is compressed or not (requires php zlib)
     *
     * @var bool
     */
    private $compression = false;

    /**
     * -1 to 9, indicates zlib compression level: -1 = default level 6, 0 = no compression, 9 maximum compression
     *
     * @var int
     */
    private $compressionLevel = -1;

    /**
     * Constructs this backend
     *
     * @param string $context FLOW3's application context
     * @param array $options Configuration options - depends on the actual backend
     * @throws \InvalidArgumentException
     * @api
     */
    public function __construct($context, array $options = [])
    {
        if (!isset($options['predis'])) {
            throw new \InvalidArgumentException('Invalid cache backend option "predis" for backend of type "' . get_class($this) . '"',
                1231267498);
        }

        if (!isset($options['predis']['parameters'])) {
            throw new \InvalidArgumentException('Invalid cache backend option "predis[parameters]" for backend of type "' . get_class($this) . '"',
                1231267498);
        }

        if (!isset($options['predis']['options'])) {
            throw new \InvalidArgumentException('Invalid cache backend option "predis[options]" for backend of type "' . get_class($this) . '"',
                1231267498);
        }

        try {
            $this->redisClient = new Client($options['predis']['parameters'], $options['predis']['options']);
        } catch (\InvalidArgumentException $e) {
            \TYPO3\CMS\Core\Utility\GeneralUtility::sysLog('Could not connect to redis server.', 'core', \TYPO3\CMS\Core\Utility\GeneralUtility::SYSLOG_SEVERITY_ERROR);
            throw new \InvalidArgumentException($e);
        }


        // Now remove predis specific options and call default constructor
        unset($options['predis']);
        parent::__construct($context, $options);
    }

    /**
     * Saves data in the cache.
     *
     * @param string $entryIdentifier An identifier for this specific cache entry
     * @param string $data The data to be stored
     * @param array $tags Tags to associate with this cache entry. If the backend does not support tags, this option can be ignored.
     * @param int $lifetime Lifetime of this cache entry in seconds. If NULL is specified, the default lifetime is used. "0" means unlimited lifetime.
     * @return void
     * @throws \TYPO3\CMS\Core\Cache\Exception if no cache frontend has been set.
     * @throws \TYPO3\CMS\Core\Cache\Exception\InvalidDataException if the data is not a string
     * @api
     */
    public function set($entryIdentifier, $data, array $tags = [], $lifetime = null)
    {
        if (!$this->canBeUsedInStringContext($entryIdentifier)) {
            throw new \InvalidArgumentException('The specified identifier is of type "' . gettype($entryIdentifier) . '" which can\'t be converted to string.',
                1377006651);
        }
        if (!is_string($data)) {
            throw new \TYPO3\CMS\Core\Cache\Exception\InvalidDataException('The specified data is of type "' . gettype($data) . '" but a string is expected.',
                1279469941);
        }
        $lifetime = $lifetime === null ? $this->defaultLifetime : $lifetime;
        if (!is_integer($lifetime)) {
            throw new \InvalidArgumentException('The specified lifetime is of type "' . gettype($lifetime) . '" but an integer or NULL is expected.',
                1279488008);
        }
        if ($lifetime < 0) {
            throw new \InvalidArgumentException('The specified lifetime "' . $lifetime . '" must be greater or equal than zero.',
                1279487573);
        }

        $expiration = $lifetime === 0 ? self::FAKED_UNLIMITED_LIFETIME : $lifetime;
        if ($this->compression) {
            $data = gzcompress($data, $this->compressionLevel);
        }

        $this->redisClient->setex(self::IDENTIFIER_DATA_PREFIX . $entryIdentifier, $expiration, $data);

        $addTags = $tags;
        $removeTags = [];
        $existingTags = $this->redisClient->sMembers(self::IDENTIFIER_TAGS_PREFIX . $entryIdentifier);
        if (!empty($existingTags)) {
            $addTags = array_diff($tags, $existingTags);
            $removeTags = array_diff($existingTags, $tags);
        }
        if (!empty($removeTags) || !empty($addTags)) {

            $this->redisClient->multi();

            foreach ($removeTags as $tag) {
                $this->redisClient->srem(self::IDENTIFIER_TAGS_PREFIX . $entryIdentifier, $tag);
                $this->redisClient->srem(self::TAG_IDENTIFIERS_PREFIX . $tag, $entryIdentifier);
            }
            foreach ($addTags as $tag) {
                $this->redisClient->sadd(self::IDENTIFIER_TAGS_PREFIX . $entryIdentifier, [$tag]);
                $this->redisClient->sadd(self::TAG_IDENTIFIERS_PREFIX . $tag, [$entryIdentifier]);
            }

            $this->redisClient->exec();
        }
    }

    /**
     * Loads data from the cache.
     *
     * @param string $entryIdentifier An identifier which describes the cache entry to load
     * @return mixed The cache entry's content as a string or FALSE if the cache entry could not be loaded
     * @api
     */
    public function get($entryIdentifier)
    {
        if (!$this->canBeUsedInStringContext($entryIdentifier)) {
            throw new \InvalidArgumentException('The specified identifier is of type "' . gettype($entryIdentifier) . '" which can\'t be converted to string.',
                1377006652);
        }

        $storedEntry = $this->redisClient->get(self::IDENTIFIER_DATA_PREFIX . $entryIdentifier);

        if ($this->compression && (string)$storedEntry !== '') {
            $storedEntry = gzuncompress($storedEntry);
        }

        return $storedEntry;
    }

    /**
     * Checks if a cache entry with the specified identifier exists.
     *
     * @param string $entryIdentifier An identifier specifying the cache entry
     * @return bool TRUE if such an entry exists, FALSE if not
     * @api
     */
    public function has($entryIdentifier)
    {
        if (!$this->canBeUsedInStringContext($entryIdentifier)) {
            throw new \InvalidArgumentException('The specified identifier is of type "' . gettype($entryIdentifier) . '" which can\'t be converted to string.',
                1377006653);
        }

        return $this->redisClient->exists(self::IDENTIFIER_DATA_PREFIX . $entryIdentifier);
    }

    /**
     * Removes all cache entries matching the specified identifier.
     * Usually this only affects one entry but if - for what reason ever -
     * old entries for the identifier still exist, they are removed as well.
     *
     * @param string $entryIdentifier Specifies the cache entry to remove
     * @return bool TRUE if (at least) an entry could be removed or FALSE if no entry was found
     * @api
     */
    public function remove($entryIdentifier)
    {
        if (!$this->canBeUsedInStringContext($entryIdentifier)) {
            throw new \InvalidArgumentException('The specified identifier is of type "' . gettype($entryIdentifier) . '" which can\'t be converted to string.',
                1377006654);
        }
        $elementsDeleted = false;

        if ($this->redisClient->exists(self::IDENTIFIER_DATA_PREFIX . $entryIdentifier)) {
            $assignedTags = $this->redisClient->sMembers(self::IDENTIFIER_TAGS_PREFIX . $entryIdentifier);
            $this->redisClient->multi();

            foreach ($assignedTags as $tag) {
                $this->redisClient->srem(self::TAG_IDENTIFIERS_PREFIX . $tag, $entryIdentifier);
            }
            $this->redisClient->del([
                self::IDENTIFIER_DATA_PREFIX . $entryIdentifier,
                self::IDENTIFIER_TAGS_PREFIX . $entryIdentifier
            ]);
            $this->redisClient->exec();
            $elementsDeleted = true;
        }

        return $elementsDeleted;
    }

    /**
     * Removes all cache entries of this cache.
     *
     * @return void
     * @api
     */
    public function flush()
    {
        $this->redisClient->flushdb();
    }

    /**
     * Does garbage collection
     *
     * @return void
     * @api
     */
    public function collectGarbage()
    {
        $identifierToTagsKeys = $this->redisClient->keys(self::IDENTIFIER_TAGS_PREFIX . '*');
        foreach ($identifierToTagsKeys as $identifierToTagsKey) {

            list(, $identifier) = explode(':', $identifierToTagsKey);

            // Check if the data entry still exists
            if (!$this->redisClient->exists((self::IDENTIFIER_DATA_PREFIX . $identifier))) {
                $tagsToRemoveIdentifierFrom = $this->redis->sMembers($identifierToTagsKey);
                $this->redisClient->multi();
                $this->redisClient->del([$identifierToTagsKey]);
                foreach ($tagsToRemoveIdentifierFrom as $tag) {
                    $this->redisClient->srem(self::TAG_IDENTIFIERS_PREFIX . $tag, $identifier);
                }
                $this->redisClient->exec();
            }
        }
    }

    /**
     * Removes all cache entries of this cache which are tagged by the specified tag.
     *
     * @param string $tag The tag the entries must have
     * @return void
     * @api
     */
    public function flushByTag($tag)
    {
        if (!$this->canBeUsedInStringContext($tag)) {
            throw new \InvalidArgumentException('The specified tag is of type "' . gettype($tag) . '" which can\'t be converted to string.',
                1377006656);
        }

        $identifiers = $this->redisClient->smembers(self::TAG_IDENTIFIERS_PREFIX . $tag);
        if (!empty($identifiers)) {
            $this->removeIdentifierEntriesAndRelations($identifiers, [$tag]);
        }
    }

    /**
     * Finds and returns all cache entry identifiers which are tagged by the
     * specified tag
     *
     * @param string $tag The tag to search for
     * @return array An array with identifiers of all matching entries. An empty array if no entries matched
     * @api
     */
    public function findIdentifiersByTag($tag)
    {
        if (!$this->canBeUsedInStringContext($tag)) {
            throw new \InvalidArgumentException('The specified tag is of type "' . gettype($tag) . '" which can\'t be converted to string.',
                1377006655);
        }

        $foundIdentifiers = [];
        $foundIdentifiers = $this->redisClient->smembers([self::TAG_IDENTIFIERS_PREFIX . $tag]);

        return $foundIdentifiers;
    }

    /**
     * Enable data compression
     *
     * @param bool $compression TRUE to enable compression
     * @return void
     * @throws \InvalidArgumentException if compression parameter is not of type boolean
     * @api
     */
    public function setCompression($compression)
    {
        if (!is_bool($compression)) {
            throw new \InvalidArgumentException('The specified compression of type "' . gettype($compression) . '" but a boolean is expected.',
                1289679153);
        }
        $this->compression = $compression;
    }

    /**
     * Set data compression level.
     * If compression is enabled and this is not set,
     * gzcompress default level will be used.
     *
     * @param int $compressionLevel -1 to 9: Compression level
     * @return void
     * @throws \InvalidArgumentException if compressionLevel parameter is not within allowed bounds
     * @api
     */
    public function setCompressionLevel($compressionLevel)
    {
        if (!is_integer($compressionLevel)) {
            throw new \InvalidArgumentException('The specified compression of type "' . gettype($compressionLevel) . '" but an integer is expected.',
                1289679154);
        }
        if ($compressionLevel >= -1 && $compressionLevel <= 9) {
            $this->compressionLevel = $compressionLevel;
        } else {
            throw new \InvalidArgumentException('The specified compression level must be an integer between -1 and 9.',
                1289679155);
        }
    }

    /**
     * Helper method to catch invalid identifiers and tags
     *
     * @param mixed $variable Variable to be checked
     * @return bool
     */
    protected function canBeUsedInStringContext($variable)
    {
        return is_scalar($variable) || (is_object($variable) && method_exists($variable, '__toString'));
    }

    /**
     * Helper method for flushByTag()
     * Gets list of identifiers and tags and removes all relations of those tags
     *
     * Scales O(1) with number of cache entries
     * Scales O(n^2) with number of tags
     *
     * @param array $identifiers List of identifiers to remove
     * @param array $tags List of tags to be handled
     * @return void
     */
    protected function removeIdentifierEntriesAndRelations(array $identifiers, array $tags)
    {
        // Set a temporary entry which holds all identifiers that need to be removed from
        // the tag to identifiers sets
        $uniqueTempKey = 'temp:' . StringUtility::getUniqueId();
        $prefixedKeysToDelete = [$uniqueTempKey];
        $prefixedIdentifierToTagsKeysToDelete = [];

        foreach ($identifiers as $identifier) {
            $prefixedKeysToDelete[] = self::IDENTIFIER_DATA_PREFIX . $identifier;
            $prefixedIdentifierToTagsKeysToDelete[] = self::IDENTIFIER_TAGS_PREFIX . $identifier;
        }

        foreach ($tags as $tag) {
            $prefixedKeysToDelete[] = self::TAG_IDENTIFIERS_PREFIX . $tag;
        }

        $tagToIdentifiersSetsToRemoveIdentifiersFrom = $this->redisClient->sunion($prefixedIdentifierToTagsKeysToDelete);

        // Remove the tag to identifier set of the given tags, they will be removed anyway
        $tagToIdentifiersSetsToRemoveIdentifiersFrom = array_diff($tagToIdentifiersSetsToRemoveIdentifiersFrom, $tags);

        // Diff all identifiers that must be removed from tag to identifiers sets off from a
        // tag to identifiers set and store result in same tag to identifiers set again
        $this->redisClient->multi();

        foreach ($identifiers as $identifier) {
            $this->redisClient->sadd($uniqueTempKey, [$identifier]);
        }

        foreach ($tagToIdentifiersSetsToRemoveIdentifiersFrom as $tagToIdentifiersSet) {
            $this->redisClient->sdiffstore(self::TAG_IDENTIFIERS_PREFIX . $tagToIdentifiersSet,
                [self::TAG_IDENTIFIERS_PREFIX . $tagToIdentifiersSet, $uniqueTempKey]);
        }

        $this->redisClient->del(array_merge($prefixedKeysToDelete, $prefixedIdentifierToTagsKeysToDelete));
        $this->redisClient->exec();
    }
}