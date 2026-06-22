package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaTransactionListener {
    @Autowired
        private UserRepository userRepository;

        @Autowired
        private TransactionRepository transactionRepository;

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-group"
    )
    public void listen(Transaction transaction) {

        // System.out.println(
        //         "Received transaction: "
        //                 + transaction.getAmount()
        // );

        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if(sender == null || recipient == null) {
            return;
        }

        if(sender.getBalance() < transaction.getAmount()) {
            return;
        }

        sender.setBalance(
        sender.getBalance() - transaction.getAmount()
        );

        recipient.setBalance(
                recipient.getBalance() + transaction.getAmount()
        );

        userRepository.save(sender);
        userRepository.save(recipient);

        System.out.println(
        sender.getName() + " balance = " + sender.getBalance()
    );

        System.out.println(
            recipient.getName() + " balance = " + recipient.getBalance()
        );

        TransactionRecord record =
        new TransactionRecord(
                sender,
                recipient,
                transaction.getAmount()
        );

        transactionRepository.save(record);

    }
}