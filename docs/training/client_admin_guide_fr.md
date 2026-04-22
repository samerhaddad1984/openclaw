# Guide de l'administrateur client — OtoCPA

Ce guide s'adresse à l'administrateur du portail côté client — la
personne chez l'entreprise cliente qui coordonne les téléversements
pour le cabinet CPA. Si vous êtes contributeur (non-admin), voir
[client_contributor_guide_fr.md](client_contributor_guide_fr.md).

## 1. Accepter votre invitation

1. Vous recevez un courriel de votre CPA avec le sujet « vous invite
   à soumettre des reçus sur OtoCPA ».
2. Cliquez **Accepter l'invitation**. Le lien expire dans 14 jours.
3. La page d'acceptation est bilingue; utilisez le commutateur en
   haut à droite pour basculer FR / EN.
4. Après acceptation, vous êtes redirigé vers votre **portail
   personnel**.

## 2. Votre portail personnel

Votre URL personnelle est unique et privée; gardez-la confidentielle.
Le portail contient :

- **Téléverser** : reçus, factures, relevés (photo, PDF, par lot).
- **Mes documents** : historique de ce que vous avez soumis.
- **Messages** : fil bidirectionnel avec votre CPA.
- **Gérer l'équipe** (admin seulement) : inviter / suspendre /
  retirer vos collègues.

## 3. Inviter vos collègues

Onglet **Gérer l'équipe → Inviter** :

1. Entrer le courriel + le nom complet.
2. Choisir le rôle :
   - **Admin** : peut inviter d'autres personnes, suspendre, retirer.
   - **Contributeur** : peut seulement téléverser et envoyer des
     messages.
3. Optionnel : choisir la langue FR ou EN pour le courriel
   d'invitation.
4. Cliquer **Envoyer l'invitation**. Le courriel part dans les 5
   minutes (réessais automatiques en cas d'échec).

## 4. Gérer votre équipe

Pour chaque membre :

- **Suspendre** : bloque l'accès sans supprimer l'historique.
- **Réactiver** : rétablit l'accès d'un utilisateur suspendu.
- **Retirer** : invalide les jetons immédiatement; l'historique
  reste pour l'audit.
- **Changer le rôle** : promouvoir un contributeur en admin, ou
  l'inverse.

⚠ **Vous ne pouvez pas vous retirer vous-même.** Demandez à votre CPA
si vous devez quitter l'organisation.

## 5. Téléverser des reçus

Trois options :

- **Photo** : caméra du téléphone (iOS / Android).
- **PDF** : glisser-déposer ou sélection de fichier.
- **WhatsApp** : envoyer la photo au numéro WhatsApp de votre CPA;
  elle apparaît dans votre file.

Chaque téléversement peut avoir une **note** (ex. « facture
d'épicerie / grocery invoice »).

## 6. Connecter votre banque (si pas via QuickBooks)

Si votre comptabilité n'est pas déjà dans QuickBooks avec flux
bancaires :

1. Onglet **Banque → Connecter**.
2. Choisir votre institution dans Plaid (portail sécurisé Plaid, pas
   de mot de passe stocké chez votre CPA).
3. OtoCPA importe les transactions automatiquement.

**Aucune action requise** si votre CPA utilise déjà QBO avec vos
flux bancaires — OtoCPA en tire directement.

## 7. Envoyer un message à votre CPA

Onglet **Messages → Nouveau** :

- Tapez votre question.
- Cliquez **Envoyer**.
- Le message apparaît immédiatement dans votre fil; votre CPA reçoit
  une notification.

## 8. Vérifier le statut de vos soumissions

Onglet **Mes documents** :

- **En file / Queued** : reçu, en traitement automatique.
- **En révision / In review** : examiné par un employé du CPA.
- **Approuvé / Posted** : validé et comptabilisé.
- **Rejeté / Rejected** : renvoyé avec une note; action requise.

## 9. Enregistrer les numéros WhatsApp de votre équipe

Chaque personne de votre équipe peut enregistrer **un** numéro
WhatsApp (téléphone d'entreprise ou personnel) sur sa ligne du
portail. Une fois enregistré, elle peut envoyer des photos de reçus
directement au numéro WhatsApp partagé de votre cabinet, et chaque
document est attribué à *son* nom plutôt qu'à un anonyme.

Sur la page **Gérer votre équipe**, chaque ligne a un champ
WhatsApp. Collez le numéro (n'importe quel format — nous le
normalisons en `+1 (514) 555-0100`) et cliquez *Enregistrer*. La
validation en direct vous dit si le numéro est libre ou déjà pris
par un autre utilisateur.

**Notes de sécurité :**

- **Seuls les numéros enregistrés fonctionnent.** Les messages en
  provenance de numéros inconnus sont rejetés avec une réponse
  bilingue « non enregistré » — rien d'anonyme n'atterrit dans
  votre file.
- **Retirer un utilisateur révoque son accès WhatsApp.** Le
  prochain message de ce téléphone recevra « votre accès WhatsApp
  a été révoqué ». *Suspendre* est similaire mais temporaire.
- **Les numéros sont uniques à l'échelle du système.** Si
  quelqu'un d'autre dans un autre cabinet a déjà enregistré le
  même téléphone, le formulaire vous prévient pour éviter une
  collision silencieuse chez Twilio.

## Questions fréquentes

**Q : J'ai perdu mon lien personnel.**
R : Demandez à un autre admin de votre équipe, ou contactez votre
CPA qui peut régénérer votre jeton.

**Q : Un collègue a quitté l'entreprise.**
R : Allez dans **Gérer l'équipe → [Personne] → Retirer**. Accès
révoqué sur-le-champ; ses téléversements restent dans votre
historique.

**Q : Mon CPA voit-il mes identifiants bancaires?**
R : Non. Plaid gère l'authentification; OtoCPA ne reçoit que les
transactions, jamais vos identifiants.

## Besoin d'aide

- **Votre CPA** (première ligne) : via l'onglet **Messages**.
- **Support OtoCPA** : support@otocpa.com
