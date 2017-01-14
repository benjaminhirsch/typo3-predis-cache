# TYPO3 Redis caching backend
Instead of using the PHP extension "redis" like the build-in TYPO3 Redis caching backend,
this one uses the predis (https://github.com/nrk/predis) library - so no PHP extension is 
required!

Most of this library is identical to the build-in redis backend, I simply changed the 
redis calls.

## Installation
#####Composer 
```php
composer require b3n/typo3-predis-cache
```

## Usage
#####Example singe server configuration
```php
return [
   'SYS' => [
      'caching' => [
         'cache_pages' => [
            'backend' => \B3N\TYPO3\Cache\Backend\RedisBackend::class,
            'options' => [
                'defaultLifetime' => 0,
                'predis' => [
                    'options' => [],
                    'parameters' => [
                        'tcp://127.0.0.1:6379',
                    ],
                ],
            ],
         ],
      ],
   ],
];
```

#####Example configuration with replication
```php
return [
   'SYS' => [
      'caching' => [
         'cache_pages' => [
            'backend' => \B3N\TYPO3\Cache\Backend\RedisBackend::class,
            'options' => [
                'defaultLifetime' => 0,
                'predis' => [
                    'options' => [
                        'replication' => true,
                    ],
                    'parameters' => [
                        'tcp://127.0.0.1:6379?alias=master',
                        'tcp://127.0.0.1:6379',
                    ],
                ],
            ],
         ],
      ],
   ],
];
```

You can pass every setting like you would when you use predis directly. 
For more information please have a look here: https://github.com/nrk/predis#how-to-install-and-use-predis