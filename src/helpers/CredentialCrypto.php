<?php
namespace burrow\Burrow\helpers;

use Craft;

/**
 * Encrypts Burrow credentials at rest using {@see \craft\services\Security::encryptByKey()}
 * (Craft's security key). Values are prefixed so legacy plaintext rows can still be read.
 */
final class CredentialCrypto
{
    private const PREFIX = 'b1:';

    public const INFO_INGESTION_KEY = 'burrow-credential/ingestion-key';

    public const INFO_CONNECTION_API_KEY = 'burrow-credential/connection-api-key';

    public static function seal(string $plain, string $info): string
    {
        if ($plain === '') {
            return '';
        }

        $cipherBinary = Craft::$app->getSecurity()->encryptByKey($plain, null, $info);

        return self::PREFIX . base64_encode($cipherBinary);
    }

    public static function unseal(string $stored, string $info): string
    {
        if ($stored === '') {
            return '';
        }

        if (!str_starts_with($stored, self::PREFIX)) {
            return $stored;
        }

        $raw = base64_decode(substr($stored, strlen(self::PREFIX)), true);
        if ($raw === false) {
            Craft::warning('Burrow credential could not be base64-decoded.', __METHOD__);

            return '';
        }

        try {
            $plain = Craft::$app->getSecurity()->decryptByKey($raw, null, $info);
        } catch (\Throwable $e) {
            Craft::warning('Burrow credential decrypt failed: ' . $e->getMessage(), __METHOD__);

            return '';
        }
        if ($plain === false) {
            Craft::warning('Burrow credential could not be decrypted (check security key or data integrity).', __METHOD__);

            return '';
        }

        return (string)$plain;
    }
}
