package Distiller.ORM;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class CryptoKeyIds
{
    private @Value("${SMOK_CRYPTO_NEW_KEY_ID:2}") int newKeyId;
    private @Value("${SMOK_CRYPTO_OLD_KEY_ID:1}") int oldKeyId;

    public int getNewKeyId() {
        return newKeyId;
    }

    public void setNewKeyId(int newKeyId) {
        this.newKeyId = newKeyId;
    }

    public int getOldKeyId() {
        return oldKeyId;
    }

    public void setOldKeyId(int oldKeyId) {
        this.oldKeyId = oldKeyId;
    }
}
